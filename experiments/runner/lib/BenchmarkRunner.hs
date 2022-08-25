module BenchmarkRunner (runBenchmarks) where

import Control.Applicative (liftA2)
import Control.Monad.Reader qualified as RD
import Data.Aeson
  ( Options (constructorTagModifier, sumEncoding),
    SumEncoding (ObjectWithSingleField),
    defaultOptions,
  )
import Data.Aeson.TH (deriveJSON)
import Data.Char (toLower)
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
import Data.Yaml (decodeFileThrow)
import EventGenerators
  ( OperatorBenchmark (..),
    generateLogForBenchmark,
    getBenchName,
  )
import Flags (Flags (..), NestedFlags (..))
import Monitors
  ( monitorName,
    monitors,
    prepareAndBenchmarkMonitor,
  )
import Process (withTmpDir)
import System.FilePath ((</>))
import UnliftIO (MonadIO (liftIO))
import UnliftIO.Resource (runResourceT)

newtype OperatorBenchmarks = OperatorBenchmarks [OperatorBenchmark] deriving (Show)

$( deriveJSON
     defaultOptions
       { constructorTagModifier = map toLower,
         sumEncoding = ObjectWithSingleField
       }
     ''OperatorBenchmarks
 )

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

runMonitorBenchmark bench builder = do
  reps <- RD.ask <&> f_nes_flags <&> bf_reps
  withTmpDir $ \d ->
    let log_f = d </> "log"
        sig_f = d </> "sig"
        fo_f = d </> "fo"
     in liftIO (generateLogForBenchmark log_f sig_f fo_f bench)
          >> foldrM
            ( \(m, i) builder ->
                runSingleBenchmark sig_f fo_f log_f m i builder
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
                  out_bench_name = getBenchName bench,
                  out_rep = i,
                  out_time = t
                }
            )
    monitorItPairs reps =
      liftA2 (,) monitors [0 .. (reps - 1)]

runBenchmarks' = do
  outfile <- RD.asks (bf_out . f_nes_flags)
  confpath <- RD.asks (bf_config . f_nes_flags)
  OperatorBenchmarks confs <- decodeFileThrow confpath
  foldrM runMonitorBenchmark mempty confs
    >>= (liftIO . TL.writeFile outfile)
      . TL.decodeUtf8
      . Csv.encodeDefaultOrderedByName

runBenchmarks :: Flags -> IO ()
runBenchmarks =
  RD.runReaderT runBenchmarks'
