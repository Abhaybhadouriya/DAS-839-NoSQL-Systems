import os
import glob

def get_timestamp(filename):
    """Extract timestamp from filename (e.g., '102564.txt' -> 102564)."""
    return int(os.path.splitext(filename)[0])

def read_files():
    """Read all .txt files and store words by index."""
    word_map = {}
    
    for file in glob.glob("*.txt"):  # Get all .txt files in the current directory
        timestamp = get_timestamp(file)
        
        with open(file, 'r', encoding='utf-8') as f:
            words = f.read().split()
            
            for i, word in enumerate(words):
                if i not in word_map or word_map[i][0] < timestamp:
                    word_map[i] = (timestamp, word)
    
    return word_map

def main():
    word_map = read_files()
    
    for index in sorted(word_map.keys()):
        print(f"Index {index}: {word_map[index][1]} (Timestamp: {word_map[index][0]})")

if __name__ == "__main__":
    main()

