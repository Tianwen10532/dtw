
import dtw

dtw.init()


@dtw.remote
class PiWorker:
    def __init__(self):
        pass
    def compute(self, num_samples:str) -> str:
        import random
        num_samples = eval(num_samples)
        
        inside_circle = 0
        for _ in range(num_samples):
            x, y = random.random(), random.random()
            if x**2 + y**2 <= 1:
                inside_circle += 1
        ret = inside_circle
        return str(ret)
    

num_workers = 4

sample_per_worker = 1000000
num_samples = dtw.put(str(sample_per_worker))

workers = [PiWorker.remote() for _ in range(num_workers)]
futures = [worker.compute.remote(num_samples) for worker in workers]
results = [dtw.get(future) for future in futures]

results = [eval(r) for r in results]
# print(results)

total_inside = sum(results)
total_samples = num_workers * sample_per_worker
pi_estimate = 4 * total_inside / total_samples

print(pi_estimate)

for worker in workers:
    worker.free()