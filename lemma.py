from nltk.stem import WordNetLemmatizer
from nltk.corpus import wordnet
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from multiprocessing import cpu_count, Pool as m_pool
from multiprocessing.dummy import Pool as t_pool
from threading import Thread
from queue import Queue
from time import time
from re import compile


def lemma(term):
    return term, ' '.join([WordNetLemmatizer().lemmatize(t) for t in term.split(' ')])


def lemma_queue(term, q):
    q.put(term, ' '.join([WordNetLemmatizer().lemmatize(t) for t in term.split(' ')]))


def main():
    t0 = time()
    terms = []
    pool_size = cpu_count()
    wordnet.ensure_loaded()
    punct = compile('[^A-z0-9 ]')

    with open('./data/searches', 'r') as fi:
        for term in fi:
            terms.append(punct.sub('', term.strip()))
    list_size = len(terms) // pool_size
    t_dif = time() - t0

    # Serial -- 3 seconds
    for term in terms:
        print(lemma(term))
    t1 = time()
    print(t1 - t0)
    print()

    # Futures Concurrent Threads -- 9 seconds
    with ThreadPoolExecutor(pool_size) as a:
        b = a.map(lemma, terms, chunksize=list_size)
        for z in b:
            z
    t2 = time()
    print(t2 - t1 + t_dif)
    print()

    # Futures Concurrent Multiprocess -- 9 seconds
    with ProcessPoolExecutor(pool_size) as a:
        b = a.map(lemma, terms, chunksize=list_size)
        for z in b:
            z
    t3 = time()
    print(t3 - t2 + t_dif)
    print()

    # Threading -- 20 seconds
    q = Queue()
    threads = []
    for term in terms:
        t = Thread(target=lemma_queue, args=(term, q))
        t.daemon = True
        t.start()
        threads.append(t)

    for thread in threads:
        thread.join()

    while not q.empty():
        q.get()
    t4 = time()
    print(t4 - t3 + t_dif)
    print()

    # Multiprocessing Pool -- 11 seconds
    p = m_pool(pool_size)
    p.map(lemma, terms)
    t5 = time()
    print(t5 - t4 + t_dif)
    print()

    # Threadding Pool -- 3 seconds
    p = t_pool(pool_size)
    p.map(lemma, terms)
    t6 = time()
    print(t6 - t5 + t_dif)
    print()


if __name__ == "__main__":
    main()
