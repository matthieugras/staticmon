module Monitors
  ( monitors,
    monpoly,
    verimon,
    staticmon,
    prepareAndRunMonitor,
    prepareAndBenchmarkMonitor,
    monitorName,
  )
where

import Control.Monad.Reader qualified as RD
import Data.Function ((&))
import Data.Functor ((<&>))
import Data.Text qualified as T
import Data.Text.IO qualified as T
import Flags (Flags (..))
import Shelly.Helpers (FlagSh (..), shellyWithFlags)
import Shelly.Lifted

default (T.Text)

data Monitor = forall s.
  Monitor
  { prepareMonitor ::
      forall a.
      ( FilePath -> -- Sig file
        FilePath -> -- Formula file
        (s -> FlagSh a) -> -- Callback
        FlagSh a
      ),
    runBenchmark ::
      ( s -> -- Some state
        FilePath -> -- Sig file
        FilePath -> -- Formula file
        FilePath -> -- Log file
        FlagSh (Double) -- Execution time
      ),
    runMonitor ::
      forall a.
      ( s -> -- Some state
        (FilePath -> FlagSh a) -> -- Action to run with the output
        FilePath -> -- Sig file
        FilePath -> -- Formula file
        FilePath -> -- Log file
        FlagSh a
      ),
    monitorName :: T.Text
  }

prepareAndRunMonitor ::
  Monitor ->
  (FilePath, FilePath, FilePath) ->
  (FilePath -> FlagSh ()) ->
  FlagSh ()
prepareAndRunMonitor Monitor {..} (s, f, l) a = do
  prepareMonitor s f $ \state ->
    runMonitor state a s f l

prepareAndBenchmarkMonitor :: Monitor -> (FilePath, FilePath, FilePath) -> FlagSh Double
prepareAndBenchmarkMonitor Monitor {..} (s, f, l) = do
  prepareMonitor s f $ \state ->
    runBenchmark state s f l

monitors :: [Monitor]
monitors = [monpoly, verimon, staticmon, cppmon]

monpoly = Monitor {..}
  where
    prepareMonitor _ _ c = c ()
    runBenchmark _ s f l = benchmarkMonpoly False s f l
    runMonitor _ a s f l = monitorMonpoly a False s f l
    monitorName = "monpoly"

verimon = Monitor {..}
  where
    prepareMonitor _ _ c = c ()
    runBenchmark _ s f l = benchmarkMonpoly True s f l
    runMonitor _ a s f l = monitorMonpoly a True s f l
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
      runSaveOut "monpoly" (["-cppmon"] ++ monpolyBaseOpts s f) c
    runBenchmark f s _ l =
      runDiscardOut "cppmon" (opts s f l)
        & time <&> fst
    runMonitor f a s _ l =
      runSaveOut "cppmon" (opts s f l) a
    monitorName = "cppmon"

staticmon = Monitor {..}
  where
    prepareMonitor s f c = do
      basedir <- f_mon_path <$> RD.ask
      b <- f_build_dir <$> RD.ask
      let header_dir = basedir </> "src" </> "staticmon" </> "input_formula"
          opts = ["-explicitmon", "-explicitmon_prefix", toTextIgnore header_dir]
          builddir = toTextIgnore $ basedir </> b
      runMonpoly opts s f
      run_ "ninja" ["--quiet", "-C", builddir]
      c (builddir </> "bin" </> "staticmon")
    runBenchmark exe _ _ l =
      runDiscardOut exe ["--log", toTextIgnore l]
        & time
        <&> fst
    runMonitor exe a _ _ l =
      runSaveOut exe ["--log", toTextIgnore l] a

    monitorName = "staticmon"

monpolyBaseOpts s f = ["-formula", toTextIgnore f, "-sig", toTextIgnore s]

runSaveOut exe opts a =
  withTmpDir $ \d ->
    let outf = d <> "output"
     in run exe opts
          & print_stdout False
          >>= liftIO . T.writeFile outf
          >> a outf

runDiscardOut exe opts =
  print_stdout False $ run_ exe opts

monitorMonpoly a v s f l =
  runSaveOut "monpoly" (opts ++ monpolyBaseOpts s f) a
  where
    opts = (if v then ["-verified"] else []) ++ ["-log", toTextIgnore l]

benchmarkMonpoly v s f l =
  runMonpoly opts s f & time <&> fst
  where
    opts = (if v then ["-verified"] else []) ++ ["-log", toTextIgnore l]

runMonpoly opts s f =
  runDiscardOut "monpoly" (opts ++ monpolyBaseOpts s f)
