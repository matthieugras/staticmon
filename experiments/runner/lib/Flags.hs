module Flags (Interval, Bnd (..), Flags (..), NestedFlags (..), parseFlags) where

import Data.Text qualified as T
import Data.Text.Read qualified as R
import Options.Applicative

data Bnd = Inf | Bounded Int deriving (Show)

type Interval = (Int, Bnd)

data NestedFlags
  = TestFlags
      { tf_ranges :: [Interval]
      }
  | BenchFlags
      { bf_reps :: Int,
        bf_out :: FilePath
      }
  deriving (Show)

data Flags = Flags
  { f_nes_flags :: NestedFlags,
    f_mon_path :: FilePath,
    f_build_dir :: T.Text
  }
  deriving (Show)

parseFlags :: IO (Flags)
parseFlags = customExecParser (prefs noBacktrack) with_desc
  where
    opts =
      ( \f_mon_path f_build_dir f_nes_flags ->
          Flags {..}
      )
        <$> strArgument
          ( metavar "STATICMON_PREFIX"
              <> help "Path of staticmon root"
          )
        <*> strOption
          ( long "build_folder"
              <> short 'b'
              <> metavar "FOLDER_NAME"
              <> value "builddir"
              <> help "Name of the Cmake build folder"
          )
        <*> nestedFlagsParser

    with_desc =
      info
        (opts <**> helper)
        ( fullDesc
            <> progDesc "Run tests or benchmarks for staticmon"
        )

nestedFlagsParser :: Parser NestedFlags
nestedFlagsParser =
  hsubparser
    ( ( command
          "test"
          ( info testFlagsParser (progDesc "Run tests")
          )
      )
        <> ( command
               "bench"
               (info benchFlagsParser (progDesc "Run benchmarks"))
           )
    )

benchFlagsParser :: Parser NestedFlags
benchFlagsParser =
  BenchFlags
    <$> option
      auto
      ( long "repetitions"
          <> short 'r'
          <> metavar "INT"
          <> value 1
          <> help "Number of repetitions"
      )
      <*> strOption
        ( long "output_file"
            <> short 'o'
            <> value "~/out.csv"
            <> help "Where to write the measurements"
        )

testFlagsParser :: Parser NestedFlags
testFlagsParser =
  TestFlags
    <$> option
      (maybeReader parseIntervals)
      ( long "tests"
          <> short 's'
          <> metavar "[((n1|*) - (n2|*)) | *]"
          <> value [(0, Inf)]
          <> help "Range of tests to run"
      )

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

parseIntervals :: String -> Maybe [Interval]
parseIntervals s =
  let l = T.split (== ',') $ T.pack s
   in traverse parseInterval l
