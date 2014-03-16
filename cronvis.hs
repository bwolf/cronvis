-- test.hs my 1st test in haskell to translate transform.py to haskell

import Text.Regex
import Data.Char (isSpace)
import Data.List

trim = f . f
    where f = reverse . dropWhile isSpace

removeNiceCmd s = subRegex re s ""
    where re = mkRegex "/usr/bin/nice( -n [[:digit:]]+)?[[:blank:]]*"

removeCronStartCmd s = subRegex re s ""
    where re = mkRegex "/usr/projekt/tools/bin/cron.start2?[[:blank:]]*"

stringTrimmedUpToChar c s = trim $ s'
    where s' = takeWhile (\l -> l /= c) s

simplifyCmd s = removeNiceCmd . removeCronStartCmd . stringTrimmedUpToChar '#' .
                stringTrimmedUpToChar '>' $ s

filterCrond :: [String] -> [String]
filterCrond = filter (isInfixOf "/USR/SBIN/CRON")
