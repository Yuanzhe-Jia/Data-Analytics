# 1
# entropy based random detector

import math
from collections import Counter

def calculate_entropy(string):
    """
    Calculate the Shannon entropy of a given string.
    """
    # Count the frequency of each character
    freq = Counter(string)
    total_chars = len(string)

    # Compute the entropy
    entropy = -sum((count / total_chars) * math.log2(count / total_chars) for count in freq.values())
    return entropy

def is_random_based_on_entropy(string, threshold=4.0):
    """
    Determine if a string is random based on its entropy.

    Parameters:
    - string: The input string to evaluate.
    - threshold: The entropy value above which a string is considered random.

    Returns:
    - bool: True if the string is random, False otherwise.
    """
    entropy = calculate_entropy(string)
    print(f"Entropy of the string: {entropy:.2f}")
    return entropy > threshold

# Example usage
string1 = "dfwnlrtgpnlx,nfg%2p9230usl"
string2 = "hellohellohello"

print(f"Is string1 random? {is_random_based_on_entropy(string1)}")
print(f"Is string2 random? {is_random_based_on_entropy(string2)}")

# 2
# N-gram based random detector

from collections import Counter

def extract_ngrams(string, n):
    """
    Extract n-grams from the input string.

    Parameters:
    - string: The input string.
    - n: The size of the n-grams to extract.

    Returns:
    - List of n-grams.
    """
    return [string[i:i+n] for i in range(len(string) - n + 1)]

def calculate_ngram_repetition(string, n):
    """
    Calculate the repetition rate of n-grams in the string.

    Parameters:
    - string: The input string.
    - n: The size of the n-grams to analyze.

    Returns:
    - float: The repetition rate (0 to 1).
    """
    ngrams = extract_ngrams(string, n)
    freq = Counter(ngrams)

    # Count repeated n-grams
    repeated_ngrams = sum(count for count in freq.values() if count > 1)
    total_ngrams = len(ngrams)

    # Return repetition rate
    return repeated_ngrams / total_ngrams if total_ngrams > 0 else 0

def is_random_based_on_ngram_analysis(string, n=3, threshold=0.2):
    """
    Determine if a string is random based on n-gram analysis.

    Parameters:
    - string: The input string to evaluate.
    - n: The size of the n-grams to analyze.
    - threshold: The repetition rate threshold below which the string is considered random.

    Returns:
    - bool: True if the string is random, False otherwise.
    """
    repetition_rate = calculate_ngram_repetition(string, n)
    print(f"Repetition rate for {n}-grams: {repetition_rate:.2f}")
    return repetition_rate < threshold

# Example usage
string1 = "dfwnlrtgpnlx,nfg%2p9230usl"
string2 = "DLKJSDFDKLSJFSDKLJFLSDKFJOAIUSF"

print(f"Is string1 random? {is_random_based_on_ngram_analysis(string1)}")
print(f"Is string2 random? {is_random_based_on_ngram_analysis(string2)}")
