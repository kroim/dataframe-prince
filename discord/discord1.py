import numpy as np


def pig_latin(input):
    def isvowel(s):
        return s.startswith("a") or s.startswith("e") or s.startswith("i") or s.startswith("o") or s.startswith("u")

    s = str(input).lower()
    suffix = "ay"
    vsuffix = "way"

    if not s.isalpha():
        return None
    elif isvowel(s):
        return s+vsuffix
    else:
        index = 0
        vowels = ["a", "e", "i", "o", "u"]
        for i in range(s.__len__()):
            if s[i] in vowels:
                index = i
                break
        if index == 0:
            return s + vsuffix
        else:
            return s[index:] + s[:index] + suffix


res = np.append((3, 8), (14,), 0)




