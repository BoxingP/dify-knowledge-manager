import base64
import re

replacement_str_mapping = {
    "A": 2,
    "B": 3,
    "C": 4,
    "D": 5,
    "E": 6,
    "F": 7,
    "G": 8,
    "H": 9,
    "I": 10,
    "J": 11,
    "K": 12,
    "L": 13,
    "M": 14,
    "N": 15,
    "O": 16,
    "P": 17,
    "Q": 18,
    "R": 19,
    "S": 20,
    "T": 21,
    "U": 22,
    "V": 23,
    "W": 24,
    "X": 25,
    "Y": 26,
    "Z": 27,
    "a": 28,
    "b": 29,
    "c": 30,
    "d": 31,
    "e": 32,
    "f": 33,
    "g": 34,
    "h": 35,
    "i": 36,
    "j": 37,
    "k": 38,
    "l": 39,
    "m": 40,
    "n": 41,
    "o": 42,
    "p": 43,
    "q": 44,
    "r": 45,
    "s": 46,
    "t": 47,
    "u": 48,
    "v": 49,
    "w": 50,
    "x": 51,
    "y": 52,
    "z": 53,
    "0": 54,
    "1": 55,
    "2": 56,
    "3": 57,
    "4": 58,
    "5": 59,
    "6": 60,
    "7": 61,
    "8": 62,
    "9": 63,
    "-": 64,
    "_": 65,
}


def decode_ppv3(mangled_url):
    parsed_url = mangled_url

    p = re.compile("__(.*)__;(.*)!!")
    ps = p.search(parsed_url)

    if ps is None:
        print("%s is not a valid URL?" % parsed_url)
        return parsed_url

    url = ps.group(1)
    replacement_b64 = ps.group(2)

    if len(replacement_b64) == 0:
        return url

    replacement_str = (base64.urlsafe_b64decode(replacement_b64 + "==")).decode('utf-8')
    replacement_list = list(replacement_str)
    url_list = list(url)

    offset = 0
    save_bytes = 0
    for m in re.finditer(r"(?<!\*)\*(?!\*)|\*{2}[A-Za-z0-9-_]", url):
        if m.group(0) == "*":
            url_list[m.start() + offset] = replacement_list.pop(0)
        elif m.group(0).startswith("**"):
            num_bytes = replacement_str_mapping[m.group(0)[-1]]
            if save_bytes != 0:
                num_bytes += save_bytes
                save_bytes = 0

            replacement_chars = list()

            i = 0
            while i < num_bytes:
                replacement_char = replacement_list.pop(0)
                replacement_chars.append(replacement_char)
                i += len(replacement_char.encode('utf-8'))

                if len(replacement_list) != 0:
                    next_replacement_char = replacement_list[0]
                    next_replacement_char_size = len(
                        next_replacement_char.encode('utf-8')
                    )

                    if next_replacement_char_size > (num_bytes - i):
                        save_bytes = num_bytes - i
                        i += save_bytes

            url_list[m.start() + offset: m.end() + offset] = replacement_chars

            offset += len(replacement_chars) - 3
        else:
            print("shouldn't get here")
            pass

    cleaned_url = "".join(url_list)

    return cleaned_url
