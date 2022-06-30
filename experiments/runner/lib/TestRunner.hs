module TestRunner (runTests) where

import Control.Monad (filterM, when)
import Control.Monad.Reader qualified as RD
import Data.Foldable (Foldable (foldl'))
import Data.Functor ((<&>))
import Data.List qualified as List
import Data.Text qualified as T
import Flags (Bnd (..), Flags (..), NestedFlags (..))
import Fmt ((+|), (|+))
import Monitors (verifyMonitor)
import Process (echo, ls, test_d)
import System.FilePath ((</>))
import System.ProgressBar qualified as PG
import UnliftIO (MonadIO (liftIO))
import UnliftIO.Resource (runResourceT)

default (T.Text)

runTest pg i = do
  monpath <- RD.asks f_mon_path
  let testDir = monpath </> "experiments" </> "tests" </> show i
  let args = (testDir </> "sig", testDir </> "fo", testDir </> "log")
  runResourceT $
    verifyMonitor args
      >>= either
        (const $ error $ "test " +| i |+ " failed")
        ( const $
            liftIO $ PG.incProgress pg 1
        )

runIntervalTest pg (lb, ub) =
  mapM_ (runTest pg) [lb .. ub]

runTests' = do
  monpath <- RD.asks f_mon_path
  let testDir = monpath </> "experiments" </> "tests"
  nfolders <- countTests testDir
  when (nfolders == 0) $
    error "no test cases"
  ranges <- RD.ask <&> f_nes_flags <&> tf_ranges
  let intvs = map (translateInterval nfolders) ranges
  if intervalsOverlap intvs
    then error "overlapping intervals"
    else do
      echo "Running tests ..."
      let intvs_sum =
            foldl'
              ( \acc (b1, b2) ->
                  acc + (b2 - b1 + 1)
              )
              0
              intvs
      pg <- liftIO $ setupProgresBar intvs_sum
      mapM_
        (runIntervalTest pg)
        intvs
  where
    countTests d = ls d >>= filterM test_d <&> length

    barStyle =
      PG.defStyle
        { PG.stylePostfix = PG.exact,
          PG.styleWidth = PG.ConstantWidth 50
        }

    setupProgresBar maxprogres =
      PG.newProgressBar
        barStyle
        10
        ( PG.Progress
            { progressDone = 0,
              progressTodo = maxprogres,
              progressCustom = ()
            }
        )

    intervalsOverlap l =
      let ls = List.sort l
       in any
            (\((_, a2), (b1, _)) -> b1 <= a2)
            (zip ls (tail ls))

    translateBnd _ (Bounded b) = b
    translateBnd nfolders Inf = nfolders - 1

    translateInterval nfolders (a, b) = (a, translateBnd nfolders b)

runTests :: Flags -> IO ()
runTests = RD.runReaderT runTests'
