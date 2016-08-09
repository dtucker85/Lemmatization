from nltk.stem import WordNetLemmatizer
from nltk.corpus import wordnet
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from multiprocessing import cpu_count, Pool as Process_Pool
from multiprocessing.dummy import Pool as Thread_Pool
from threading import Thread
from queue import Queue
from timer import Timer
from re import compile


def lemma(term):
    return term, ' '.join([WordNetLemmatizer().lemmatize(t) for t in term.split(' ')])


def lemma_queue(term, q):
    q.put(term, ' '.join([WordNetLemmatizer().lemmatize(t) for t in term.split(' ')]))


def serial_lemma(terms):
    lemmas = [lemma(term) for term in terms]


def futures_thread_lema(terms):
    pool_size = cpu_count()
    list_size = len(terms) // pool_size
    with ThreadPoolExecutor(pool_size) as a:
        lemmas = a.map(lemma, terms, chunksize=list_size)


def futures_process_lema(terms):
    pool_size = cpu_count()
    list_size = len(terms) // pool_size
    with ProcessPoolExecutor(pool_size) as a:
        lemmas = a.map(lemma, terms, chunksize=list_size)


def threads_lemma(terms):
    q = Queue()
    threads = []
    lemmas = []
    for term in terms:
        t = Thread(target=lemma_queue, args=(term, q))
        t.daemon = True
        t.start()
        threads.append(t)

    for thread in threads:
        thread.join()

    while not q.empty():
        lemmas.append(q.get())


def process_pool_lemma(terms):
    pool_size = cpu_count()
    p = Process_Pool(pool_size)
    lemas = p.map(lemma, terms)


def thread_pool_lemma(terms):
    pool_size = cpu_count()
    p = Thread_Pool(pool_size)
    lemas = p.map(lemma, terms)


def main():
    wordnet.ensure_loaded()
    punct = compile('[^A-z0-9 ]+')

    with open('./data/searches', 'r') as fi:
        terms = [punct.sub(' ', term.strip()) for term in fi]

    functs = {"serial lemma": serial_lemma,
              "futures thread lemma": futures_thread_lema,
              "futures process lemma": futures_process_lema,
              "threads lemma": threads_lemma,
              "pool process lemma": process_pool_lemma,
              "thread process lemma": thread_pool_lemma}

    timer = Timer(functs, terms, 100000000, cpu_count())
    timer.run()


if __name__ == "__main__":
    main()
