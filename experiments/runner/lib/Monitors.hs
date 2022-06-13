module Monitors (Monitor (..), monitors, monpoly, verimon, staticmon, prepareAndRunMonitor) where

import Control.Monad.Reader qualified as RD
import Data.Text qualified as T
import Data.Text.IO qualified as T
import Flags (Flags (..))
import Shelly.Helpers (FlagSh (..), shellyWithFlags)
import Shelly.Lifted

default (T.Text)

data Monitor = forall s.
  Monitor
  { prepareMonitor ::
      ( FilePath -> -- Sig file
        FilePath -> -- Formula file
        FlagSh (s) -- Some state
      ),
    runBenchmark ::
      ( s -> -- Some state
        FilePath -> -- Sig file
        FilePath -> -- Formula file
        FilePath -> -- Log file
        FlagSh (Double) -- Execution time
      ),
    runMonitor ::
      ( s -> -- Some state
        (FilePath -> FlagSh ()) -> -- Action to run with the output
        FilePath -> -- Sig file
        FilePath -> -- Formula file
        FilePath -> -- Log file
        FlagSh ()
      ),
    monitorName :: T.Text
  }

prepareAndRunMonitor Monitor {..} (s, f, l) a = do
  state <- prepareMonitor s f
  runMonitor state a s f l

monitors :: [Monitor]
monitors = [monpoly, verimon, staticmon]

monpoly = Monitor {..}
  where
    prepareMonitor _ _ = return ()
    runBenchmark _ s f l = benchmarkMonpoly False s f l
    runMonitor _ a s f l = monitorMonpoly a False s f l
    monitorName = "monpoly"

verimon = Monitor {..}
  where
    prepareMonitor _ _ = return ()
    runBenchmark _ s f l = benchmarkMonpoly True s f l
    runMonitor _ a s f l = monitorMonpoly a True s f l
    monitorName = "verimon"

staticmon = Monitor {..}
  where
    prepareMonitor s f = do
      basedir <- f_mon_path <$> RD.ask
      b <- f_build_dir <$> RD.ask
      let header_dir = basedir </> "src" </> "staticmon" </> "input_formula"
          opts = ["-explicitmon", "-explicitmon_prefix", toTextIgnore header_dir]
          builddir = toTextIgnore $ basedir </> b
      runMonpoly opts s f
      run_ "ninja" ["--quiet", "-C", builddir]
      return (builddir </> "bin" </> "staticmon")
    runBenchmark exe _ _ l =
      fst <$> (time $ print_stdout False $ run_ exe ["--log", toTextIgnore l])
    runMonitor exe a _ _ l =
      withTmpDir
        ( \d ->
            let outf = d </> "monitor_out"
             in run_ exe ["--log", toTextIgnore l, "--vpath", toTextIgnore outf]
                  >> a outf
        )
    monitorName = "staticmon"

monpolyBaseOpts s f = ["-formula", toTextIgnore f, "-sig", toTextIgnore s]

monitorMonpoly a v s f l =
  withTmpDir
    ( \d ->
        let outf = d </> "monitor_out"
         in ( ( print_stdout False $
                  run "monpoly" ((monpolyBaseOpts s f) ++ opts)
              )
                >>= (liftIO . T.writeFile outf)
                >> a outf
            )
    )
  where
    opts = (if v then ["-verified"] else []) ++ ["-log", toTextIgnore l]

benchmarkMonpoly v s f l =
  fst <$> (time $ runMonpoly opts s f)
  where
    opts = (if v then ["-verified"] else []) ++ ["-log", toTextIgnore l]

runMonpoly opts s f =
  print_stdout False $
    run_
      "monpoly"
      ((monpolyBaseOpts s f) ++ opts)
