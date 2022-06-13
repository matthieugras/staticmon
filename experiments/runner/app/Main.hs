module Main (main) where

import BenchmarkRunner (runBenchmarks)
import Flags (Flags (..), NestedFlags (..), parseFlags)
import TestRunner (runTests)

main :: IO ()
main =
  parseFlags
    >>= ( \case
            f@(Flags (TestFlags _) _ _) -> runTests f
            f -> runBenchmarks f
        )
