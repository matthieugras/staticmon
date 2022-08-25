{-# LANGUAGE ParallelListComp #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}

{-# HLINT ignore "Use second" #-}

module EventGenerators
  ( generateRandomLog,
    AndOptions (..),
    OrOptions (..),
    AntiJoinOptions (..),
    AndConfig (..),
    OrConfig (..),
    AntiJoinConfig (..),
    andBenchGen,
    getBenchName,
    OperatorBenchmark (..),
    generateLogForBenchmark,
  )
where

import Control.Applicative (Applicative (liftA2))
import Control.Exception.Extra (assertIO)
import Control.Monad (forM_, replicateM, when)
import Control.Monad.Extra (replicateM_)
import Control.Monad.Reader (ReaderT)
import Control.Monad.Reader qualified as R
import Control.Monad.State
import Data.Aeson
  ( Options (constructorTagModifier, fieldLabelModifier, sumEncoding),
    SumEncoding (ObjectWithSingleField),
    defaultOptions,
  )
import Data.Aeson.TH (deriveJSON)
import Data.Char (toLower)
import Data.Map qualified as M
import Data.Random (shuffle)
import Data.Random.List qualified as L
import Data.Random.RVar (runRVar)
import Data.Random.Vector qualified as V
import Data.Text qualified as T
import Data.Vector ((!?))
import Data.Vector qualified as V
import EventPrinting
import Flags (Flags (..), NestedFlags (..))
import Fmt (Builder, (+|), (|+))
import SignatureParser (SigType (..), Signature)
import System.Random.Stateful (globalStdGen, uniformRM)

data AndOptions
  = Distinct (Int, Int)
  | Subset (Bool, Int, Int)
  | FixedCommon (Int, Int, Int)
  deriving (Show)

$( deriveJSON
     defaultOptions
       { constructorTagModifier = map toLower,
         sumEncoding = ObjectWithSingleField
       }
     ''AndOptions
 )

data AndConfig = AndConfig
  { ac_lsize :: Int,
    ac_rsize :: Int,
    ac_opts :: AndOptions
  }
  deriving (Show)

$( deriveJSON
     defaultOptions
       { fieldLabelModifier = drop 3,
         constructorTagModifier = map toLower,
         sumEncoding = ObjectWithSingleField
       }
     ''AndConfig
 )

data OrOptions
  = SameLayout
  | Shuffled
  deriving (Show)

$( deriveJSON
     defaultOptions
       { constructorTagModifier = map toLower,
         sumEncoding = ObjectWithSingleField
       }
     ''OrOptions
 )

data OrConfig = OrConfig
  { oc_lsize :: Int,
    oc_rsize :: Int,
    oc_numvars :: Int,
    oc_opts :: OrOptions
  }
  deriving (Show)

$( deriveJSON
     defaultOptions
       { fieldLabelModifier = drop 3,
         constructorTagModifier = map toLower,
         sumEncoding = ObjectWithSingleField
       }
     ''OrConfig
 )

data AntiJoinOptions
  = SubsetSequence (Int, Int, Int)
  | RandomSubset (Int, Int, Int)
  deriving (Show)

$( deriveJSON
     defaultOptions
       { constructorTagModifier = map toLower,
         sumEncoding = ObjectWithSingleField
       }
     ''AntiJoinOptions
 )

data AntiJoinConfig = AntiJoinConfig
  { aj_lsize :: Int,
    aj_rsize :: Int,
    aj_opts :: AntiJoinOptions
  }
  deriving (Show)

$( deriveJSON
     defaultOptions
       { fieldLabelModifier = drop 3,
         constructorTagModifier = map toLower,
         sumEncoding = ObjectWithSingleField
       }
     ''AntiJoinConfig
 )

data OperatorBenchmark
  = AndOperator AndConfig
  | OrOperator OrConfig
  | AntiJoinOperator AntiJoinConfig
  deriving (Show)

$( deriveJSON
     defaultOptions
       { constructorTagModifier = map toLower,
         sumEncoding = ObjectWithSingleField
       }
     ''OperatorBenchmark
 )

getBenchName :: OperatorBenchmark -> T.Text
getBenchName op = case op of
  AndOperator AndConfig {..} ->
    let prefix :: Builder = "and" +| ac_lsize |+ "_" +| ac_rsize |+ ""
        suffix :: Builder = case ac_opts of
          Distinct (n1, n2) -> "cartesian" +| n1 |+ "_" +| n2 |+ ""
          Subset (left_subset, n1, n2) -> "subset" +| left_subset |+ "_" +| n1 |+ "_" +| n2 |+ ""
          FixedCommon (nc, n1, n2) -> "common" +| nc |+ "_" +| n1 |+ "_" +| n2 |+ ""
     in "" +| prefix |+ "_" +| suffix |+ ""
  OrOperator OrConfig {..} ->
    let prefix :: Builder = "or" +| oc_lsize |+ "_" +| oc_rsize |+ ""
        suffix :: Builder = case oc_opts of
          SameLayout -> "same" +| oc_numvars |+ ""
          Shuffled -> "shuffled" +| oc_numvars |+ ""
     in "" +| prefix |+ "_" +| suffix |+ ""
  AntiJoinOperator AntiJoinConfig {..} ->
    let prefix :: Builder = "or" +| aj_lsize |+ "_" +| aj_rsize |+ ""
        suffix :: Builder = case aj_opts of
          SubsetSequence (nc, n1, n2) -> "seq" +| nc |+ "_" +| n1 |+ "_" +| n2 |+ ""
          RandomSubset (nc, n1, n2) -> "random" +| nc |+ "_" +| n1 |+ "_" +| n2 |+ ""
     in "" +| prefix |+ "_" +| suffix |+ ""

intArgs = map Intgr

randomEvent arity ub =
  intArgs <$> replicateM arity (uniformRM (0, ub) globalStdGen)

outputRandomEvents name num arity ub =
  forM_ [0 .. (num -1)] $
    const $
      randomEvent arity ub >>= outputNewEvent name

binPredicateCommonVars :: Int -> Int -> Int -> IO ([Int], [Int])
binPredicateCommonVars nphi npsi nc = do
  assertIO (nc >= npsi && nc >= nphi)
  let l1 = [0 .. (nphi - nc - 1)]
      l2 = [(nphi - nc) .. (nphi - nc + npsi - nc - 1)]
      prefix = [(nphi - nc + npsi - nc) .. (nphi - nc + npsi - 1)]
      r1 = shuffle (prefix ++ l1)
      r2 = shuffle (prefix ++ l2)
  res1 <- runRVar r1 globalStdGen
  res2 <- runRVar r2 globalStdGen
  return (res1, res2)

binPredicateSubsetVars :: Int -> Int -> Bool -> IO ([Int], [Int])
binPredicateSubsetVars nphi npsi left_subset =
  let l = [0 .. (varSize - 1)]
      r1 = shuffle l
      r2 = L.shuffleNofM subsetSize varSize l
   in do
        l1 <- runRVar r1 globalStdGen
        l2 <- runRVar r2 globalStdGen
        if left_subset
          then return (l2, l1)
          else return (l1, l2)
  where
    varSize = if left_subset then npsi else nphi
    subsetSize = if left_subset then nphi else npsi

binPredicateDistinctVars :: Int -> Int -> IO ([Int], [Int])
binPredicateDistinctVars nphi npsi =
  let r1 = shuffle [0 .. (nphi - 1)]
      r2 = shuffle [nphi .. (nphi + npsi - 1)]
   in do
        l1 <- runRVar r1 globalStdGen
        l2 <- runRVar r2 globalStdGen
        return (l1, l2)

binColGroups :: [Int] -> [Int] -> (V.Vector Int, V.Vector Int)
binColGroups phivars psivars =
  let m1 = M.fromList $ zip phivars [0 :: Int ..]
      m2 = M.fromList $ zip psivars [0 :: Int ..]
      mc = M.intersectionWith (,) m1 m2
      (cidx1, cidx2) = unzip $ map snd $ M.toList mc
   in (V.fromList cidx1, V.fromList cidx2)

projectVec v idxs =
  V.unfoldr
    ( \i -> case v !? i of
        Just a -> Just (a, i + 1)
        Nothing -> Nothing
    )
    0

genAndCommonTables AndConfig {..} n1 n2 cidx1 cidx2 = do
  levs <- V.replicateM ac_lsize (V.fromList <$> randomEvent n1 100000)
  revs <- V.replicateM ac_rsize (V.fromList <$> randomEvent n2 100000)
  let levsproj = V.map (`projectVec` cidx1) levs
      revsproj = V.map (`projectVec` cidx2) revs
      lrvar = V.randomElement levsproj
      rrvar = V.randomElement revsproj
  levrep <-
    V.mapM
      ( \v -> do
          vproj <- runRVar rrvar globalStdGen
          return $ V.toList $ V.update_ v cidx1 vproj
      )
      levs
  revrep <-
    V.mapM
      ( \v -> do
          vproj <- runRVar lrvar globalStdGen
          return $ V.toList $ V.update_ v cidx2 vproj
      )
      revs
  return (levrep, revrep)

andBenchTpGen conf@AndConfig {..} cidx1 cidx2 =
  case ac_opts of
    Distinct (n1, n2) -> do
      outputRandomEvents "P" ac_lsize n1 100000
      outputRandomEvents "Q" ac_rsize n2 100000
    Subset (_, n1, n2) -> commonCase n1 n2
    FixedCommon (_, n1, n2) -> commonCase n1 n2
  where
    commonCase n1 n2 = do
      (l, r) <- genAndCommonTables conf n1 n2 cidx1 cidx2
      V.forM_ l (outputNewEvent "P")
      V.forM_ r (outputNewEvent "Q")

andBenchGen log_f sig_f fo_f conf@AndConfig {..} =
  do
    (phivars, psivars) <- case ac_opts of
      Distinct (n1, n2) -> liftIO $ binPredicateDistinctVars n1 n2
      Subset (left_subset, n1, n2) -> liftIO $ binPredicateSubsetVars n1 n2 left_subset
      FixedCommon (nc, n1, n2) -> liftIO $ binPredicateCommonVars n1 n2 nc
    let (cidx1, cidx2) = binColGroups phivars psivars
    withPrintState log_f $ do
      forM_ [0 .. 100000] $ \i -> do
        newDb i
        andBenchTpGen conf cidx1 cidx2
      endOutput

orBenchGen log_f sig_f fo_f OrConfig {..} =
  let n = oc_numvars
   in do
        (phivars, psivars) <- case oc_opts of
          SameLayout -> return ([0 :: Int .. (n -1)], [0 .. (n -1)])
          Shuffled ->
            let rvar = shuffle [0 :: Int .. (n -1)]
             in liftA2 (,) (runRVar rvar globalStdGen) (runRVar rvar globalStdGen)
        withPrintState log_f $ do
          forM_ [0 .. 100000] $ \i -> do
            newDb i
            outputRandomEvents "P" oc_lsize n 100000
            outputRandomEvents "Q" oc_rsize n 100000
          endOutput

generateLogForBenchmark :: FilePath -> FilePath -> FilePath -> OperatorBenchmark -> IO ()
generateLogForBenchmark log_f sig_f fo_f conf =
  case conf of
    AndOperator andconf -> andBenchGen log_f sig_f fo_f andconf
    OrOperator orconf -> orBenchGen log_f sig_f fo_f orconf
    _ -> fail "unsupported mode"

generateRandomLog :: FilePath -> Signature -> ReaderT Flags IO ()
generateRandomLog fp sig =
  let arrsig = map (\(a, b) -> (a, length b)) sig
   in do
        when (any (any (/= IntTy) . snd) sig) $
          error "only integer signatures supported"
        Flags {f_nes_flags = RandomTestFlags {..}, ..} <- R.ask
        R.liftIO . withPrintState fp $ do
          forM_ [0 .. rt_num_ts] $ \ts ->
            replicateM_ rt_db_per_ts $ do
              newDb (fromIntegral ts)
              forM_ arrsig $ \(name, arity) ->
                replicateM_ rt_events_per_db $
                  randomEvent arity rt_ub
                    >>= outputNewEvent name
          endOutput
