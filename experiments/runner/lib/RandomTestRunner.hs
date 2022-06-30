module RandomTestRunner (runRandomTests) where

import Control.Concurrent.Async.Lifted qualified as Async
import Control.Concurrent.Lifted (setNumCapabilities)
import Control.Concurrent.QSemN.Lifted
import Control.Exception.Lifted (bracket_, finally, handle, onException)
import Control.Monad (forM_, forever, replicateM_, void)
import Control.Monad.Reader (ReaderT)
import Control.Monad.Reader qualified as RD
import Data.Function ((&))
import Data.Functor ((<&>))
import Data.Text qualified as T
import Data.Text.IO qualified as T
import Data.UUID as Uid
import Data.UUID.V4 as Uid
import EventGenerators (generateRandomLog)
import Flags (Flags (..), NestedFlags (..))
import Fmt ((+|), (|+))
import GHC.Conc (getNumProcessors)
import Monitors
  ( Monitor (..),
    VerificationFailed,
    monpoly,
    prepareAndRunMonitor,
    prepareMonitor,
    runMonitor,
    staticmon,
    verifyMonitor,
  )
import Shelly (RunFailed)
import Shelly.Helpers (FlagSh, shellyWithFlags)
import Shelly.Lifted
import SignatureParser (parseSig)
import System.IO (IOMode (ReadMode), hFileSize, withFile)

printFile f =
  liftIO (T.readFile f) >>= echo

forUntil_ (l : ls) a =
  a l >>= \b ->
    if b
      then return ()
      else forUntil_ ls a
forUntil_ [] a = return ()

withGenerateFormula c =
  withTmpDir $ \d ->
    let f = d </> "out.mfotl"
        s = d </> "out.sig"
        base = toTextIgnore $ d </> "out"
     in do
          print_stderr True $ run_ "gen_fma" ["-output", base, "-past_only", "-size", "2"]
          c s f

withGeneratedLog s f c = do
  withTmpDir $ \d ->
    let l = d </> "log"
     in liftIO (T.readFile s) >>= generateRandomLog l . parseSig >> c l

withGenerateLogAndFormula c = do
  withGenerateFormula $ \s f -> withGeneratedLog s f $ \l ->
    c s f l

withGoodFormula c = do
  forUntil_ [1 .. iterlimit] $ \i -> do
    when (i == iterlimit) $
      fail "didn't find good formula in 200 iters, change params"
    withGenerateLogAndFormula $ \s f l ->
      case monpoly of
        Monitor {..} ->
          prepareMonitor s f $ \case
            Left _ -> return False
            Right state ->
              runMonitor state s f l $ \case
                Left _ -> return False
                Right outf -> do
                  sz <- liftIO . withFile outf ReadMode $ \h ->
                    hFileSize h
                  if sz == 0
                    then return False
                    else c s f >> return True
  where
    iterlimit = 200 :: Int

saveFiles s f l = do
  mondir <- RD.asks f_mon_path
  let basedir = mondir </> "experiments" </> "random_test_out"
  whenM (not <$> test_d basedir) $
    mkdir basedir
  outdirname <- liftIO $ Uid.toString <$> Uid.nextRandom
  let outdir = basedir </> outdirname
  mkdir outdir
  forM_ [s, f, l] (`cp` outdir)
  echo_err $ "failing test case saved in " +| outdir |+ ""

runSingleTest sem = do
  num_reps <- RD.asks (rt_reps_per_formula . f_nes_flags)
  handle @_ @VerificationFailed (const $ return ()) $
    replicateM_ num_reps $
      withGoodFormula $ \s f ->
        withGeneratedLog s f $ \l ->
          verifyMonitor staticmon (s, f, l)
            `onException` saveFiles s f l

dispatchSingleTest sem = do
  flip onException (signalQSemN sem 1) $ do
    Async.async (finally (runSingleTest sem) (signalQSemN sem 1))
      >>= Async.link

runRandomTests' :: FlagSh ()
runRandomTests' = do
  cpus <- (`div` 2) <$> liftIO getNumProcessors
  setNumCapabilities cpus
  sem <- newQSemN cpus
  forever (waitQSemN sem 1 >> dispatchSingleTest sem)

-- runRandomTests' :: FlagSh ()
-- runRandomTests' = do
--   cpus <- liftIO getNumProcessors
--   setNumCapabilities cpus
--   sem <- newQSemN cpus
--   dispatchSingleTest sem
--   waitQSemN sem cpus

runRandomTests :: Flags -> IO ()
runRandomTests =
  shelly
    . silently
    . tracing False
    . shellyWithFlags runRandomTests'
