module Main (main) where

import Control.Monad (filterM)
import Data.Foldable (Foldable (foldl'))
import Data.Foldable qualified as List
import Data.List qualified as List
import Data.Maybe (fromJust)
import Data.Semigroup ((<>))
import Data.Text qualified as T
import Data.Text.Read qualified as R
import Fmt ((+|), (|+), (|++|))
import Options.Applicative
import Shelly
import System.ProgressBar qualified as PG

default (T.Text)

data Bnd = Inf | Bounded Int deriving (Show)

type Interval = (Int, Bnd)

type BndInterval = (Int, Int)

data Flags = Flags
  { f_ranges :: [Interval],
    f_build_dir :: T.Text,
    f_mon_path :: FilePath
  }
  deriving (Show)

parseBound :: T.Text -> Maybe Bnd
parseBound "*" = Just Inf
parseBound t =
  case R.decimal t of
    Right (i :: Int, "") -> Just (Bounded i)
    _ -> Nothing

parseInterval :: T.Text -> Maybe Interval
parseInterval s =
  case (T.split (== '-') s) of
    [x, y] -> do
      x_bnd <- parseBound x
      y_bnd <- parseBound y
      case (x_bnd, y_bnd) of
        (Inf, Inf) -> Nothing
        (Bounded a, Bounded b) | (a > b) -> Nothing
        (Inf, r) -> Just (0, r)
        (Bounded a, r) -> Just (a, r)
    [x] ->
      ( \case
          Bounded x -> (x, Bounded x)
          Inf -> (0, Inf)
      )
        <$> parseBound x
    _ -> Nothing

parserIntervals :: String -> Maybe [Interval]
parserIntervals s =
  let l = T.split (== ',') $ T.pack s
   in traverse parseInterval l

getFlagsParser :: Parser Flags
getFlagsParser =
  Flags
    <$> option
      (maybeReader parserIntervals)
      ( long "tests"
          <> short 's'
          <> metavar "[((n1|*) - (n2|*)) | *]"
          <> value [(0, Inf)]
          <> help "Range of tests to run"
      )
    <*> strOption
      ( long "build_folder"
          <> short 'b'
          <> metavar "FOLDER_NAME"
          <> value "builddir_debug"
          <> help "Name of the Cmake build folder"
      )
    <*> strArgument
      ( metavar "STATICMON_PREFIX"
          <> help "Path of staticmon root"
      )

intToText :: Int -> T.Text
intToText = T.pack . show

parseFlags :: IO (Flags)
parseFlags = execParser opts
  where
    opts =
      info
        (getFlagsParser <**> helper)
        ( fullDesc
            <> progDesc "Run monitor tests"
        )

runMonitors :: [T.Text] -> FilePath -> FilePath -> Sh ()
runMonitors monp_opts smon_exe log_f =
  withTmpDir
    ( \tmp_d ->
        let smon_out = tmp_d </> "staticmon_out"
            monp_out = tmp_d </> "monpoly_out"
         in do
              log_f <- toTextIgnore <$> absPath "log"
              run_
                smon_exe
                [ "--log",
                  log_f,
                  "--vpath",
                  toTextIgnore $ smon_out
                ]
              ( print_stdout False $
                  run
                    "monpoly"
                    ( monp_opts
                        ++ ["-log", log_f]
                    )
                )
                >>= (writefile monp_out)
              run_
                "diff"
                [toTextIgnore smon_out, toTextIgnore monp_out]
    )

runTest :: Flags -> PG.ProgressBar a -> Int -> Sh ()
runTest Flags {..} pg i =
  chdir
    (show i)
    ( let build_dir = toTextIgnore $ f_mon_path </> f_build_dir
          header_dir = toTextIgnore $ f_mon_path </> "src" </> "input_formula"
          exe_file = build_dir </> "bin" </> "staticmon"
       in do
            ffile <- absPath "formula.mfotl"
            sfile <- absPath "sig"
            lfile <- absPath "log"
            let monp_opts =
                  [ "-formula",
                    toTextIgnore $ ffile,
                    "-sig",
                    toTextIgnore $ sfile
                  ]
            run_
              "monpoly"
              ( monp_opts
                  ++ [ "-explicitmon",
                       "-explicitmon_prefix",
                       header_dir
                     ]
              )
            run_ "ninja" ["--quiet", "-C", build_dir]
            runMonitors monp_opts exe_file lfile
            liftIO $ PG.incProgress pg 1
    )

countFolders :: Sh (Int)
countFolders =
  pwd >>= \f ->
    length
      <$> ( (ls f)
              >>= filterM test_d
          )

runIntervalTest :: Flags -> PG.ProgressBar a -> Int -> BndInterval -> Sh ()
runIntervalTest f pg nfolders ~(lb, ub) =
  mapM_ (runTest f pg) [lb .. ub]

translateBnd :: Int -> Bnd -> Int
translateBnd nfolders (Bounded b) = b
translateBnd nfolders Inf = nfolders - 2

translateInterval :: Int -> Interval -> BndInterval
translateInterval nfolders (a, b) = (a, translateBnd nfolders b)

intervalsOverlap :: [BndInterval] -> Bool
intervalsOverlap l =
  let ls = List.sort l
   in any
        (\((_, a2), (b1, _)) -> b1 <= a2)
        (zip ls (tail ls))

barStyle =
  PG.defStyle
    { PG.stylePostfix = PG.exact,
      PG.styleWidth = PG.ConstantWidth 50
    }

runTests :: Flags -> Sh ()
runTests ~(f@Flags {..}) = do
  cd (f_mon_path </> "test")
  nfolders <- countFolders
  when (nfolders == 0) (errorExit "no test cases")
  let intvs = map (translateInterval nfolders) f_ranges
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
      pg <-
        liftIO $
          PG.newProgressBar
            barStyle
            10
            ( PG.Progress
                { progressDone = 0,
                  progressTodo = intvs_sum,
                  progressCustom = ()
                }
            )
      mapM_
        (runIntervalTest f pg nfolders)
        intvs

main :: IO ()
main = parseFlags >>= (shelly . (tracing False) . runTests)
