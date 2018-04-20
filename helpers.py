import re

"""Methods for name part extraction"""


def ignore_initials(s):
    if s is None:
        return s

    if s.endswith('.') or len(s) <= 1:  # Russian initials like 'Yu. or 'H-K.', or regular initial without '.'
        return None
    else:
        return s


def get_item_or_filler(l, pos, filler=None):
    """
    Return item of list 'l' at position 'pos' if it exists, otherwise return the 'filler' element.
    Example:
        get_item_or_filler(['a', 'b', 'c'], 1)
        >>>'b'
        get_item_or_filler(['a', 'b', 'c'], 4)
        >>>None
        get_item_or_filler(['a', 'b', 'c'], 4, 'e')
        >>>'e'
    """
    if len(l) > pos:
        return l[pos]
    else:
        return filler


def extract_first_and_middle_name(forenames, separator=None):
    if forenames is not None:
        forenames = str(forenames).lower()
        words = forenames.split(separator)
        first_name = get_item_or_filler(words, 0)
        first_name = ignore_initials(first_name)
        middle_name = get_item_or_filler(words, 1)
        middle_name = ignore_initials(middle_name)
    else:
        first_name, middle_name = None, None

    return first_name, middle_name


def replace_umlauts(s):
    if s is not None and '"' in s:
        ae = re.compile('"a')
        ue = re.compile('"u')
        oe = re.compile('"o')

        umlaute = {ae: "ä", ue: "ü", oe: "ö"}
        for k, v in umlaute.items():
            s = k.sub(v, s)
    return s
