module SignatureParser (parseSig, SigType (..), Signature) where

import Control.Applicative ((<|>))
import Data.Attoparsec.Text
import Data.Char (isAlphaNum)
import Data.Functor ((<&>))
import Data.Text qualified as T

data SigType = IntTy | FloatTy | StringTy deriving (Show, Eq)

type Signature = [(T.Text, [SigType])]

intTy = string "int" *> pure IntTy

floatTy = string "float" *> pure FloatTy

stringTy = string "string" *> pure StringTy

ty = skipSpace *> (intTy <|> floatTy <|> stringTy)

tyList = skipSpace *> (ty `sepBy'` (skipSpace *> char ','))

predName = skipSpace *> takeWhile1 (inClass "a-zA-Z0-9_-")

sigPred =
  (,) <$> predName
    <*> ( skipSpace
            *> (char '(')
            *> tyList <* skipSpace <* (char ')')
        )

sig = (many' sigPred) <* skipSpace <* endOfInput

parseSig :: T.Text -> Signature
parseSig t =
  let Right (s) = parseOnly sig t
   in s