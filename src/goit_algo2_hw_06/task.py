import requests
import re
from collections import Counter
from multiprocessing import Pool, cpu_count
import matplotlib.pyplot as plt


# 1. Завантаження тексту з URL
def fetch_text(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.text


# 2. Функція маппер — розбиває текст на слова та повертає частоти
def mapper(text_chunk):
    words = re.findall(r'\b\w+\b', text_chunk.lower())
    return Counter(words)


# 3. Функція редьюсер — об'єднує лічильники
def reducer(mapped_results):
    total = Counter()
    for part in mapped_results:
        total.update(part)
    return total


# 4. Візуалізація топ-слів
def visualize_top_words(word_counts, top_n=20):
    most_common = word_counts.most_common(top_n)
    words, counts = zip(*most_common)

    plt.figure(figsize=(12, 6))
    plt.bar(words, counts)
    plt.xticks(rotation=45)
    plt.title(f"Top {top_n} Most Frequent Words")
    plt.xlabel("Words")
    plt.ylabel("Frequency")
    plt.tight_layout()
    plt.show()


# 5. Головний блок
if __name__ == '__main__':
    url = 'https://www.gutenberg.org/files/1342/1342-0.txt'  # Наприклад, роман "Pride and Prejudice"

    print("Завантаження тексту...")
    text = fetch_text(url)

    print("Розділення тексту для MapReduce...")
    num_chunks = cpu_count()
    chunk_size = len(text) // num_chunks
    chunks = [text[i * chunk_size:(i + 1) * chunk_size] for i in range(num_chunks)]

    print("Запуск MapReduce із багатопотоковістю...")
    with Pool(processes=num_chunks) as pool:
        mapped = pool.map(mapper, chunks)
        word_counts = reducer(mapped)

    print("Візуалізація результатів...")
    visualize_top_words(word_counts, top_n=20)
