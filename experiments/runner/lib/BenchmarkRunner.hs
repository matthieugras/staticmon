module BenchmarkRunner (runBenchmarks) where

import Control.Applicative (liftA2)
import Control.Exception (throwIO)
import Control.Monad (filterM)
import Control.Monad.Reader qualified as RD
import Data.Aeson ((.:))
import Data.Aeson qualified as A
import Data.Aeson.KeyMap qualified as A
import Data.Csv
  ( DefaultOrdered (..),
    ToNamedRecord (..),
    namedRecord,
    (.=),
  )
import Data.Csv.Incremental qualified as Csv
import Data.Foldable (foldrM)
import Data.Function ((&))
import Data.Functor ((<&>))
import Data.Map qualified as M
import Data.Text qualified as T
import Data.Text.Lazy.Encoding qualified as TL
import Data.Text.Lazy.IO qualified as TL
import Data.Vector qualified as V
import EventGenerators (getGenerator)
import Flags (Flags (..), NestedFlags (..))
import Monitors (monitorName, monitors, prepareAndBenchmarkMonitor)
import Shelly.Helpers (FlagSh (..), shellyWithFlags)
import Shelly.Lifted
import System.IO (FilePath)

data MeasurementRow = MeasurementRow
  { out_monitor :: T.Text,
    out_bench_name :: T.Text,
    out_rep :: Int,
    out_time :: Double
  }

instance ToNamedRecord MeasurementRow where
  toNamedRecord MeasurementRow {..} =
    namedRecord
      [ "monitor" .= out_monitor,
        "benchmark" .= out_bench_name,
        "repetition" .= out_rep,
        "time" .= out_time
      ]

instance DefaultOrdered MeasurementRow where
  headerOrder _ = V.fromList ["benchmark", "monitor", "repetition", "time"]

data JsonBenchConfig = JsonBenchConfig
  { jb_disp_name :: T.Text,
    jb_gen :: T.Text
  }

instance A.FromJSON JsonBenchConfig where
  parseJSON (A.Object v) =
    JsonBenchConfig <$> v .: "display_name" <*> v .: "generator"

data BenchConfig = BenchConfig
  { bench_id :: Int,
    bench_disp_name :: T.Text,
    bench_gen :: (FilePath -> IO ()),
    bench_path :: FilePath
  }

collectConfigs :: FlagSh [BenchConfig]
collectConfigs =
  pwd
    >>= ls
    >>= (filterM test_d)
    <&> (zip [0 :: Int ..])
    >>= traverse (uncurry (flip chdir . readConfig))
  where
    readConfig bench_id = do
      bench_path <- pwd
      A.eitherDecodeFileStrict @JsonBenchConfig (bench_path </> "config.json")
        & liftIO
        >>= ( \case
                Left (err) -> fail err
                Right (JsonBenchConfig {..}) ->
                  let bench_gen = getGenerator jb_gen
                   in return BenchConfig {bench_disp_name = jb_disp_name, ..}
            )

runMonitorBenchmarks ::
  BenchConfig ->
  Csv.NamedBuilder MeasurementRow ->
  FlagSh (Csv.NamedBuilder MeasurementRow)
runMonitorBenchmarks BenchConfig {..} builder = do
  reps <- RD.ask <&> f_nes_flags <&> bf_reps
  withTmpDir $ \d ->
    let log_f = d </> "log"
     in (liftIO $ bench_gen log_f)
          -- >> (cp log_f "/home/grasm/logfile")
          >> foldrM
            ( \(m, i) builder ->
                let s = bench_path </> "sig"
                    f = bench_path </> "fo"
                 in runSingleBenchmark s f log_f m i builder
            )
            builder
            (monitorItPairs reps)
  where
    runSingleBenchmark s f log_f m i builder = do
      t <- prepareAndBenchmarkMonitor m (s, f, log_f)
      return $
        builder
          <> Csv.encodeNamedRecord
            ( MeasurementRow
                { out_monitor = monitorName m,
                  out_bench_name = bench_disp_name,
                  out_rep = i,
                  out_time = t
                }
            )
    monitorItPairs reps =
      liftA2 (,) monitors [0 .. (reps - 1)]

runBenchmarks' :: FlagSh ()
runBenchmarks' = do
  monpath <- f_mon_path <$> RD.ask
  outfile <- (bf_out . f_nes_flags) <$> RD.ask
  let bench_dir = monpath </> "experiments" </> "bench"
  chdir bench_dir collectConfigs
    >>= foldrM runMonitorBenchmarks mempty
    <&> Csv.encodeDefaultOrderedByName
    <&> TL.decodeUtf8
    >>= (liftIO . TL.writeFile outfile)

runBenchmarks :: Flags -> IO ()
runBenchmarks = shelly . (tracing False) . (shellyWithFlags runBenchmarks')
