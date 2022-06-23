module TestRunner (runTests) where

import Control.Monad (filterM)
import Control.Monad.Reader qualified as RD
import Data.Foldable (Foldable (foldl'))
import Data.Foldable qualified as List
import Data.Functor ((<&>))
import Data.List qualified as List
import Data.Maybe (fromJust)
import Data.Semigroup ((<>))
import Data.Text qualified as T
import Data.Text.Read qualified as R
import Flags (Bnd (..), Flags (..), Interval, NestedFlags (..))
import Fmt ((+|), (|+), (|++|))
import Monitors (staticmon, verifyMonitor)
import Shelly.Helpers (FlagSh (..), shellyWithFlags)
import Shelly.Lifted
import System.ProgressBar qualified as PG

default (T.Text)

type BndInterval = (Int, Int)

runTest pg i =
  chdir (show i) $ do
    args <- (,,) <$> (absPath "sig") <*> (absPath "fo") <*> (absPath "log")
    verifyMonitor staticmon args
    liftIO $ PG.incProgress pg 1

runIntervalTest pg nfolders (lb, ub) =
  mapM_ (runTest pg) [lb .. ub]

runTests' = do
  monpath <- f_mon_path <$> RD.ask
  cd (monpath </> "experiments" </> "tests")
  nfolders <- countFolders
  when (nfolders == 0) $
    errorExit "no test cases"
  ranges <- RD.ask <&> f_nes_flags <&> tf_ranges
  let intvs = map (translateInterval nfolders) ranges
  if intervalsOverlap intvs
    then errorExit "overlapping intervals"
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
        (runIntervalTest pg nfolders)
        intvs
  where
    countFolders = pwd >>= ls >>= filterM test_d <&> length

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

    translateBnd nfolders (Bounded b) = b
    translateBnd nfolders Inf = nfolders - 1

    translateInterval nfolders (a, b) = (a, translateBnd nfolders b)

runTests :: Flags -> IO ()
runTests =
  shelly
    . tracing False
    . shellyWithFlags runTests'
