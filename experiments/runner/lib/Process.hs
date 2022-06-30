{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}

{-# HLINT ignore "Use camelCase" #-}
module Process
  ( runDiscard,
    runPipe,
    runKeep,
    run_,
    withTmpDir,
    withTmpDirIn,
    cp,
    mkdir,
    echo,
    echoErr,
    test_d,
    ls,
    cp_r,
    bash_,
    rm_rf,
    run,
    bash,
  )
where

import Control.Monad (forM_)
import Control.Monad.Extra (ifM, whenM)
import Control.Monad.Trans.Resource (MonadResource)
import Data.Either.Combinators (mapRight)
import Data.Maybe (fromJust)
import Data.Text.IO qualified as T
import Data.UUID qualified as Uid
import Data.UUID.V4 qualified as Uid
import System.Directory qualified as Dir
import System.FilePath (takeFileName, (</>))
import System.Process.Typed qualified as Proc
import UnliftIO (IOMode (WriteMode), MonadIO (liftIO), MonadUnliftIO, bracket, withBinaryFile)
import UnliftIO.Exception (finally)
import UnliftIO.IO (hClose, stderr, stdout)
import UnliftIO.Resource (ReleaseKey, allocateU)

runDiscard exe args =
  mapRight (const ()) <$> runPipe exe args True

runKeep exe args =
  mapRight fromJust <$> runPipe exe args False

runPipe exe args discard = do
  (_, d) <- allocateTempDir
  let outf = d </> "stdout"
      outerr = d </> "stderr"
  withBinaryFile
    (outDest outf)
    WriteMode
    ( \out ->
        withBinaryFile
          outerr
          WriteMode
          ( \err ->
              let pConf =
                    Proc.setStderr (Proc.useHandleOpen err)
                      . Proc.setStdout (Proc.useHandleOpen out)
                      . Proc.setStdin Proc.nullStream
                      $ Proc.proc exe args
               in Proc.runProcess pConf
          )
    )
    >>= ( \case
            Proc.ExitSuccess -> return (Right (getReturn outf))
            Proc.ExitFailure _ -> return (Left outerr)
        )
  where
    outDest outf = if discard then "/dev/null" else outf
    getReturn outf = if discard then Nothing else Just outf

checkExit err =
  \case
    Proc.ExitSuccess -> return $ Right ()
    Proc.ExitFailure _ -> return $ Left err

bash_ cwd input cmd =
  let pConf =
        Proc.setStderr Proc.inherit
          . Proc.setStdout Proc.nullStream
          . Proc.setStdin Proc.createPipe
          . Proc.setWorkingDir cwd
          $ Proc.proc "bash" ["-c", cmd]
   in Proc.withProcessWait pConf $ \p ->
        let inPipe = Proc.getStdin p
         in do
              forM_ input (liftIO . T.hPutStrLn inPipe)
                `finally` hClose inPipe
              Proc.checkExitCode p

bash cwd input cmd = do
  (_, outerr) <- allocateTempFile
  withBinaryFile
    outerr
    WriteMode
    ( \err ->
        let pConf =
              Proc.setStderr (Proc.useHandleOpen err)
                . Proc.setStdout Proc.nullStream
                . Proc.setStdin Proc.createPipe
                . Proc.setWorkingDir cwd
                $ Proc.proc "bash" ["-c", cmd]
         in Proc.withProcessWait pConf $ \p ->
              let inPipe = Proc.getStdin p
               in do
                    forM_ input (liftIO . T.hPutStrLn inPipe)
                      `finally` hClose inPipe
                    Proc.waitExitCode p
    )
    >>= checkExit outerr

run_ exe args =
  let p =
        Proc.setStderr Proc.nullStream
          . Proc.setStdout Proc.nullStream
          . Proc.setStdin Proc.nullStream
          $ Proc.proc exe args
   in do
        Proc.runProcess_ p

run exe args = do
  (_, outerr) <- allocateTempFile
  withBinaryFile
    outerr
    WriteMode
    ( \err ->
        let p =
              Proc.setStderr (Proc.useHandleOpen err)
                . Proc.setStdout Proc.nullStream
                . Proc.setStdin Proc.nullStream
                $ Proc.proc exe args
         in Proc.runProcess p
    )
    >>= checkExit outerr

tmpNameIn destdir =
  liftIO $ (destdir </>) . Uid.toString <$> Uid.nextRandom

makeTmpDirIn destdir = do
  d <- tmpNameIn destdir
  liftIO $ Dir.createDirectory d
  return d

withTmpDirIn destdir a = do
  bracket
    (makeTmpDirIn destdir)
    (liftIO . Dir.removeDirectoryRecursive)
    a

allocateTempDir :: (MonadResource m, MonadUnliftIO m) => m (ReleaseKey, FilePath)
allocateTempDir = do
  tmpDir <- liftIO Dir.getTemporaryDirectory
  allocateU (makeTmpDirIn tmpDir) rm_rf

allocateTempFile :: (MonadResource m, MonadUnliftIO m) => m (ReleaseKey, FilePath)
allocateTempFile = do
  tmpDir <- liftIO Dir.getTemporaryDirectory
  f <- tmpNameIn tmpDir
  allocateU
    (return f)
    ( \f -> do
        whenM (liftIO $ Dir.doesPathExist f) $
          liftIO $ Dir.removePathForcibly f
    )

withTmpDir a = do
  tmpDir <- liftIO Dir.getTemporaryDirectory
  withTmpDirIn tmpDir a

cp f1 f2 = do
  liftIO $
    ifM
      (Dir.doesDirectoryExist f2)
      (Dir.copyFileWithMetadata f1 (f2 </> takeFileName f1))
      (Dir.copyFileWithMetadata f1 f2)

cp_r d1 d2 = run_ "cp" ["-r", d1, d2]

mkdir d = liftIO $ Dir.createDirectoryIfMissing False d

echo txt = liftIO $ T.hPutStrLn stdout txt

echoErr txt = liftIO $ T.hPutStrLn stderr txt

rm_rf d = liftIO $ Dir.removePathForcibly d

test_d d =
  ifM
    (liftIO $ Dir.doesPathExist d)
    (liftIO $ Dir.doesDirectoryExist d)
    (error "path does not exist")

ls d =
  map (d </>) <$> liftIO (Dir.listDirectory d)
