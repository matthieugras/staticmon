{-# LANGUAGE LambdaCase #-}

module Monitors
  ( monitors,
    monpoly,
    verimon,
    staticmon,
    prepareAndRunMonitor,
    prepareAndBenchmarkMonitor,
    verifyMonitor,
    VerificationFailed(..),
    Monitor (..),
  )
where

import Control.Exception (Exception, SomeException, mapException)
import Control.Exception.Lifted (throwIO, try)
import Control.Monad (guard)
import Control.Monad.Reader qualified as RD
import Data.Data (Typeable)
import Data.Function ((&))
import Data.Functor ((<&>))
import Data.Text qualified as T
import Data.Text.IO qualified as T
import Data.Text.Lazy.IO qualified as LT
import Flags (Flags (..))
import Shelly (RunFailed (RunFailed))
import Shelly.Helpers (FlagSh (..), shellyWithFlags)
import Shelly.Lifted

default (T.Text)

data VerificationFailed = VerificationFailed deriving (Show, Typeable)

instance Exception VerificationFailed

data Monitor = forall s.
  Monitor
  { prepareMonitor ::
      forall a.
      ( FilePath -> -- Sig file
        FilePath -> -- Formula file
        (Either RunFailed s -> FlagSh a) -> -- Action to run with the state
        FlagSh a
      ),
    runBenchmark ::
      s -> -- Some state
      FilePath -> -- Sig file
      FilePath -> -- Formula file
      FilePath -> -- Log file
      FlagSh Double,
    runMonitor ::
      forall a.
      ( s -> -- Some state
        FilePath -> -- Sig file
        FilePath -> -- Formula file
        FilePath -> -- Log file
        (Either RunFailed FilePath -> FlagSh a) -> -- Action to run with the output
        FlagSh a
      ),
    monitorName :: T.Text
  }

throwEither c e = case e of
  Left e -> throwIO e
  Right v -> c v

prepareAndRunMonitor ::
  Monitor ->
  (FilePath, FilePath, FilePath) ->
  (FilePath -> FlagSh a) ->
  FlagSh a
prepareAndRunMonitor Monitor {..} (s, f, l) a = do
  prepareMonitor s f $
    throwEither $ \state ->
      runMonitor state s f l (throwEither a)

prepareAndBenchmarkMonitor :: Monitor -> (FilePath, FilePath, FilePath) -> FlagSh Double
prepareAndBenchmarkMonitor Monitor {..} (s, f, l) = do
  prepareMonitor s f $
    throwEither $ \state ->
      runBenchmark state s f l

verifyMonitor :: Monitor -> (FilePath, FilePath, FilePath) -> FlagSh ()
verifyMonitor mon args =
  prepareAndRunMonitor verimon args $ \monp_out ->
    mapException @RunFailed (const VerificationFailed) $
      prepareAndRunMonitor mon args $ \smon_out ->
        run_ "diff" [toTextIgnore smon_out, toTextIgnore monp_out]

monitors :: [Monitor]
monitors = [monpoly, verimon, staticmon, cppmon]

monpoly = Monitor {..}
  where
    prepareMonitor _ _ c = c (Right ())
    runBenchmark _ s f l = benchmarkMonpoly False s f l
    runMonitor _ s f l a = monitorMonpoly a False s f l
    monitorName = "monpoly"

verimon = Monitor {..}
  where
    prepareMonitor _ _ c = c (Right ())
    runBenchmark _ s f l = benchmarkMonpoly True s f l
    runMonitor _ s f l a = monitorMonpoly a True s f l
    monitorName = "verimon"

cppmon = Monitor {..}
  where
    opts s f l =
      [ "--formula",
        toTextIgnore f,
        "--sig",
        toTextIgnore s,
        "--log",
        toTextIgnore l
      ]
    prepareMonitor s f c =
      runSaveOut "monpoly" ("-cppmon" : monpolyBaseOpts s f) c
    runBenchmark f s _ l =
      runDiscard "cppmon" (opts s f l)
        & time <&> fst
    runMonitor f s _ l a =
      runSaveOut "cppmon" (opts s f l) a
    monitorName = "cppmon"

staticmon = Monitor {..}
  where
    prepareMonitor s f c = do
      basedir <- RD.asks f_mon_path
      b <- RD.asks f_build_dir
      let header_dir = basedir </> "src" </> "staticmon" </> "input_formula"
          opts = ["-explicitmon", "-explicitmon_prefix", toTextIgnore header_dir]
          builddir = toTextIgnore $ basedir </> b
      runMonpoly opts s f
        >>= ( \case
                Left e -> c (Left e)
                _ -> do
                  try (run_ "ninja" ["--quiet", "-C", builddir])
                    >>= c . (const (builddir </> "bin" </> "staticmon") <$>)
            )
    runBenchmark exe _ _ l =
      runDiscard exe ["--log", toTextIgnore l]
        & time
        <&> fst
    runMonitor exe _ _ l a =
      runSaveOut exe ["--log", toTextIgnore l] a

    monitorName = "staticmon"

monpolyBaseOpts s f = ["-formula", toTextIgnore f, "-sig", toTextIgnore s]

runSaveOut exe opts a =
  withTmpDir $ \d ->
    let outf = d <> "output"
     in runPipe exe opts outf >>= a . (const outf <$>)

monitorMonpoly a v s f l =
  runSaveOut "monpoly" (opts ++ monpolyBaseOpts s f) a
  where
    opts = (["-verified" | v]) ++ ["-log", toTextIgnore l]

benchmarkMonpoly v s f l =
  runMonpoly opts s f & time <&> fst
  where
    opts = (["-verified" | v]) ++ ["-log", toTextIgnore l]

runMonpoly opts s f =
  runDiscard "monpoly" (opts ++ monpolyBaseOpts s f)

runDiscard exe args = runPipe exe args "/dev/null"

runPipe exe args outf =
  try @_ @RunFailed $
    runHandle exe (map toTextArg args) $ \out ->
      liftIO $ LT.hGetContents out >>= LT.writeFile outf
