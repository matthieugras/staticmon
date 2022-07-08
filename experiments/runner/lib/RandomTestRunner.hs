module RandomTestRunner (runRandomTests) where

import Control.Applicative (liftA2)
import Control.Concurrent.RLock qualified as Lk
import Control.Monad (forM_, forever, void, when)
import Control.Monad.Extra (whenM)
import Control.Monad.Reader (withReaderT)
import Control.Monad.Reader qualified as RD
import Control.Monad.Trans (lift)
import Control.Monad.Trans.Resource (transResourceT)
import Data.Text.IO qualified as T
import Data.UUID as Uid
import Data.UUID.V4 as Uid
import EventGenerators (generateRandomLog)
import Flags (Flags (..), NestedFlags (..))
import Fmt ((+|), (|+))
import GHC.Conc (getNumProcessors)
import Monitors
  ( VerificationFailure (..),
    monpoly,
    prepareAndRunMonitor,
    verifyMonitor,
  )
import Process
  ( bash,
    cp,
    cp_r,
    echoErr,
    makeTmpDirIn,
    mkdir,
    rm_rf,
    run,
    test_d,
    withTmpDir,
    withTmpDirIn,
  )
import SignatureParser (parseSig)
import System.FilePath (takeFileName, (</>))
import System.IO
  ( IOMode (ReadMode),
    hFileSize,
    withFile,
  )
import System.Time.Extra (sleep)
import UnliftIO (MonadIO (liftIO))
import UnliftIO.Async (replicateConcurrently_)
import UnliftIO.Concurrent (setNumCapabilities)
import UnliftIO.Exception (bracket_)
import UnliftIO.Resource (runResourceT)

forUntil_ (l : ls) a =
  a l >>= \b ->
    if b
      then return ()
      else forUntil_ ls a
forUntil_ [] _ = return ()

withDir dir = bracket_ (mkdir dir) (rm_rf dir)

getLogDir = getFlags >>= (\f -> return $ rt_out_dir (f_nes_flags f) </> "log")

getFlags = RD.asks fst

getGlobLk = RD.asks snd

transFlagsResource k =
  RD.ask
    >>= (\r -> transResourceT (withReaderT (const $ fst r)) k)

askFlags f = RD.asks (f . fst)

withGlobLk a =
  getGlobLk
    >>= ( \lk ->
            bracket_ (liftIO $ Lk.acquire lk) (liftIO $ Lk.release lk) a
        )

withoutGlobLk = withReaderT fst

copyMonSource oldmondir newmondir = do
  cp_r (oldmondir </> "cmake") newmondir
  cp_r (oldmondir </> "src") newmondir
  mapM_
    ((`cp` newmondir) . (oldmondir </>))
    [ "CMakeLists.txt",
      "conanfile.txt",
      "configure.sh",
      "setup.sh"
    ]

copyErrorToLogdir outerr = do
  logdir <- getLogDir
  let newname = logdir </> takeFileName outerr
  cp outerr logdir
  return newname

withNewMonitorRoot destdir a = do
  oldmondir <- askFlags f_mon_path
  withTmpDirIn destdir $ \newmondir -> do
    copyMonSource oldmondir newmondir
    liftIO $ sleep 1.0
    setupCmd newmondir ["C", "14", "n"] "./setup.sh"
    withGlobLk $
      setupCmd newmondir [] "./configure.sh"
    a newmondir
  where
    setupCmd newmondir args cmd =
      runResourceT $
        bash newmondir args cmd >>= \case
          Left outerr -> do
            newname <- RD.lift $ copyErrorToLogdir outerr
            RD.lift . withGlobLk . echoErr $
              "running cmd: " +| cmd |+ " in setup failed, output saved in "
                +| newname |+ ""
            error "fatal error"
          Right _ -> return ()

withGenerateFormula c =
  withTmpDir $ \d ->
    let f = d </> "out.mfotl"
        s = d </> "out.sig"
        base = d </> "out"
     in runResourceT
          ( run "gen_fma" ["-output", base, "-past_only", "-size", "3"] >>= \case
              Left outerr -> do
                newname <- RD.lift $ copyErrorToLogdir outerr
                RD.lift . withGlobLk $
                  echoErr $ "formula generator failed, output saved in " +| newname |+ ""
                error "fatal error"
              Right _ -> return ()
          )
          >> c s f

withGeneratedLog s c =
  withTmpDir $ \d ->
    let l = d </> "log"
     in liftIO (T.readFile s)
          >>= (withoutGlobLk . generateRandomLog l . parseSig) >> c l

withGenerateLogAndFormula c =
  withGenerateFormula $ \s f -> withGeneratedLog s $ \l ->
    c s f l

reportErrorWithGeneratedFormula outerr s f l = do
  outdir <- getLogDir >>= makeTmpDirIn
  mapM_ (`cp` outdir) [outerr, s, f, l]
  withGlobLk $
    echoErr $ "failed to run monpoly with generated formula, saved in: " +| outdir |+ ""
  error "fatal error"

withGoodFormula c =
  forUntil_ [1 .. iterlimit] $ \i -> do
    when (i == iterlimit) $
      error "didn't find good formula in 200 iters, change params"
    withGenerateLogAndFormula $ \s f l -> do
      foundGoodFormula <-
        runResourceT $
          transFlagsResource (prepareAndRunMonitor monpoly (s, f, l)) >>= \case
            Left outerr ->
              RD.lift $ reportErrorWithGeneratedFormula outerr s f l
            Right outf -> do
              sz <- liftIO . withFile outf ReadMode $ hFileSize
              if sz == 0
                then return False
                else return True
      if foundGoodFormula
        then c s f >> break ()
        else continue ()
  where
    continue = const $ return False
    break = const $ return True
    iterlimit = 200 :: Int

saveFailingTest s f l err didcrash stop = do
  basedir <- askFlags (rt_out_dir . f_nes_flags)
  whenM (not <$> test_d basedir) $
    mkdir basedir
  outdirname <- liftIO $ Uid.toString <$> Uid.nextRandom
  let outdir = basedir </> outdirname
  mkdir outdir
  forM_ [s, f, l, err] (`cp` outdir)
  lift $
    withGlobLk $
      echoErr $
        "test failed because of "
          +| crashTxt |+ ", failing test case saved in "
          +| outdir |+ ""
  when stop $ error "quitting because of error"
  where
    crashTxt = if didcrash then "crash" else "output differences"

runSingleTest = do
  rt_flags <- askFlags f_nes_flags
  let num_reps = rt_reps_per_formula rt_flags
  let sf = liftA2 (,) (rt_sig_path rt_flags) (rt_fo_path rt_flags)
  case sf of
    Just (s, f) -> void $ testFormula s f True
    _ -> withGoodFormula $ \s f ->
      forUntil_ [1 .. num_reps] (const $ testFormula s f False)
  where
    testFormula s f stoponerr =
      withGeneratedLog s $ \l ->
        runResourceT $ do
          transFlagsResource (verifyMonitor (s, f, l)) >>= \case
            Right () -> return False
            Left (VerificationFailed err) -> do
              saveFailingTest s f l err False stoponerr
              return True
            Left (VerificationCrash err) -> do
              saveFailingTest s f l err True stoponerr
              return True

runRandomTestsWithOverlay = do
  Flags {f_nes_flags = RandomTestFlags {..}, ..} <- getFlags
  let overlaypath = rt_out_dir </> "overlays"
  withDir overlaypath $
    withNewMonitorRoot overlaypath $ \newmonpath ->
      RD.local (\(f, lk) -> (f {f_mon_path = newmonpath}, lk)) $
        forever runSingleTest

runRandomTests' = do
  getLogDir >>= mkdir
  cpus <- liftIO $ (\procs -> procs - 2) <$> getNumProcessors
  setNumCapabilities cpus
  replicateConcurrently_ cpus runRandomTestsWithOverlay

runRandomTests :: Flags -> IO ()
runRandomTests f = do
  globLk <- liftIO Lk.new
  RD.runReaderT runRandomTests' (f, globLk)
