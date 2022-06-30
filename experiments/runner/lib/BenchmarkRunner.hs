module BenchmarkRunner (runBenchmarks) where

import Control.Applicative (liftA2)
import Control.Monad (filterM)
import Control.Monad.Reader qualified as RD
import Data.Aeson ((.:))
import Data.Aeson qualified as A
import Data.Csv
  ( DefaultOrdered (..),
    ToNamedRecord (..),
    namedRecord,
    (.=),
  )
import Data.Csv.Incremental qualified as Csv
import Data.Foldable (foldrM)
import Data.Functor ((<&>))
import Data.Text qualified as T
import Data.Text.Lazy.Encoding qualified as TL
import Data.Text.Lazy.IO qualified as TL
import Data.Vector qualified as V
import EventGenerators (getGenerator)
import Flags (Flags (..), NestedFlags (..))
import Monitors
  ( monitorName,
    monitors,
    prepareAndBenchmarkMonitor,
  )
import Process (ls, test_d, withTmpDir)
import System.FilePath ((</>))
import UnliftIO (MonadIO (liftIO))
import UnliftIO.Resource (runResourceT)

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
  parseJSON _ = error "expected object"

data BenchConfig = BenchConfig
  { bench_id :: Int,
    bench_disp_name :: T.Text,
    bench_gen :: FilePath -> IO (),
    bench_path :: FilePath
  }

collectConfigs =
  do
    monpath <- RD.asks f_mon_path
    let bench_dir = monpath </> "experiments" </> "bench"
    ls bench_dir
    >>= filterM test_d
    >>= traverse (uncurry readConfig)
      . zip [0 :: Int ..]
  where
    readConfig bench_id bench_path = do
      liftIO (A.eitherDecodeFileStrict @JsonBenchConfig (bench_path </> "config.json"))
        >>= ( \case
                Left err -> fail err
                Right JsonBenchConfig {..} ->
                  let bench_gen = getGenerator jb_gen
                   in return BenchConfig {bench_disp_name = jb_disp_name, ..}
            )

runMonitorBenchmarks BenchConfig {..} builder = do
  reps <- RD.ask <&> f_nes_flags <&> bf_reps
  withTmpDir $ \d ->
    let log_f = d </> "log"
     in liftIO (bench_gen log_f)
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
      t <- runResourceT $ prepareAndBenchmarkMonitor m (s, f, log_f)
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

runBenchmarks' = do
  outfile <- RD.asks (bf_out . f_nes_flags)
  collectConfigs
    >>= foldrM runMonitorBenchmarks mempty
    >>= (liftIO . TL.writeFile outfile)
      . TL.decodeUtf8
      . Csv.encodeDefaultOrderedByName

runBenchmarks :: Flags -> IO ()
runBenchmarks =
  RD.runReaderT runBenchmarks'
