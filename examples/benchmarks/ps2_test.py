
import dtw


@dtw.remote
class Worker:
    import ray
    import torch.nn as nn
    import torch.nn.functional as F
    
    @ray.remote
    class multi_actor:
        import torch.nn as nn
        import torch.distributed as dist

        class Net(nn.Module):
            def __init__(self):
                import torch.nn as nn
                import torch.nn.functional as F
                super().__init__()
                self.fc1 = nn.Linear(28 * 28, 512)
                self.fc2 = nn.Linear(512, 10)
            def forward(self, x):
                import torch.nn as nn
                import torch.nn.functional as F
                x = x.view(x.size(0), -1)
                x = F.relu(self.fc1(x))
                return self.fc2(x)
    

        def __init__(self, rank, world_size, master_addr, master_port):
            import os
            import torch
            import torch.distributed as dist
            import torch.nn as nn
            from torchvision import datasets, transforms
            from torch.utils.data import DataLoader, Subset

            self.rank = rank
            self.world_size = world_size

            # -----------------------------
            # 初始化 torch.distributed
            # -----------------------------
            os.environ["MASTER_ADDR"] = master_addr
            os.environ["MASTER_PORT"] = str(master_port)
            os.environ["RANK"] = str(rank)
            os.environ["WORLD_SIZE"] = str(world_size)

            self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

            dist.init_process_group(
                backend="nccl" if self.device == "cuda" else "gloo",
                rank=rank,
                world_size=world_size,
            )

            # -----------------------------
            # 模型
            # -----------------------------
            self.model = self.Net().to(self.device)
            self.optimizer = torch.optim.Adam(self.model.parameters(), lr=1e-3)
            self.loss_fn = nn.CrossEntropyLoss()

            transform = transforms.Compose([
                transforms.ToTensor(),
                transforms.Normalize((0.1307,), (0.3081,))
            ])

            dataset = datasets.MNIST(
                "./data",
                train=True,
                download=True,
                transform=transform,
            )

            # 等待 rank0 下载完成
            dist.barrier()

            # 数据并行切分
            indices = list(range(rank, len(dataset), world_size))
            subset = Subset(dataset, indices)

            self.loader = DataLoader(
                subset,
                batch_size=128,
                shuffle=True,
                num_workers=2,
                pin_memory=True,
            )

            if rank == 0:
                print(f"Actor {rank} initialized, dataset size = {len(subset)}")


        def train_one_epoch(self):
            import torch.distributed as dist

            self.model.train()
            total_loss = 0.0

            for x, y in self.loader:
                x = x.to(self.device)
                y = y.to(self.device)

                self.optimizer.zero_grad()
                out = self.model(x)
                loss = self.loss_fn(out, y)
                loss.backward()

                # 🔥 手动梯度 AllReduce
                for param in self.model.parameters():
                    dist.all_reduce(param.grad, op=dist.ReduceOp.SUM)
                    param.grad /= self.world_size

                self.optimizer.step()
                total_loss += loss.item()

            return total_loss / len(self.loader)

        def cleanup(self):
            import torch.distributed as dist
            dist.destroy_process_group()
        
        def get_grad(self):
            grads = {
                name: param.grad.detach().cpu()
                for name, param in self.model.named_parameters()
            }
            return grads
        
        def apply_gradients(self, avg_grads):
            for name, param in self.model.named_parameters():
                param.grad = avg_grads[name].to(self.device)
            self.optimizer.step()
            return None

    
    def __init__(self, gpus, worker_id, num_workers, lr=0.1, batch_size=128):
        import ray
        self.worker_id = worker_id
        self.num_workers = num_workers
        self.world_size = gpus
        self.lr=lr
        self.batch_size=batch_size

        world_size = self.world_size
        master_addr = ray.util.get_node_ip_address()
        master_port = 29500
        self.actors = [
            self.multi_actor.remote(
                rank=i,
                world_size=world_size,
                master_addr=master_addr,
                master_port=master_port,
            ) for i in range(world_size)
        ]

    def compute_gradients(self):
        import ray

        losses = ray.get([a.train_one_epoch.remote() for a in self.actors])
        loss = sum(losses) / len(losses)
        grads = ray.get(self.actors[0].get_grad.remote())
        return grads, loss
    
    def apply_grad(self, avg_grads):
        import ray
        ray.get([a.apply_gradients.remote(avg_grads) for a in self.actors])
        return None


class GradientAverager:
    def average(self, gradients_list):
        avg_grads = {}
        for k in gradients_list[0].keys():
            avg_grads[k] = sum(g[k] for g in gradients_list) / len(gradients_list)
        return avg_grads


if __name__ == "__main__":

    num_workers = 2
    ps = GradientAverager()

    worker1 = Worker.party("dtwroute").remote(1, 0, num_workers,0.1, 128)
    worker2 = Worker.party("dtwroute").remote(2, 0, num_workers,0.1, 128)

    # worker1 = Worker.party("dtwroute").remote(gpu=1,id=0,num_workers=2,lr=0.1, batchsize=128)
    # worker2 = Worker.party("dtwroute").remote(gpu=2,id=1,num_workers=2,lr=0.1, batchsize=128)

    for step in range(5):
        w1o = worker1.compute_gradients.remote()
        w2o1 = worker2.compute_gradients.remote()
        w2o2 = worker2.compute_gradients.remote()
        results = [dtw.get(w1o), dtw.get(w2o2)]
        grads_list, losses = zip(*results)
        avg_grads = ps.average(list(grads_list))

        was = [worker1.apply_grad.remote(avg_grads),
               worker2.apply_grad.remote(avg_grads)]
        was = [dtw.get(wa) for wa in was]
        
        print(f"Step {step}, loss: {sum(losses)/len(losses):.4f}")
    
    worker1.free()
    worker2.free()
    
    print("Training finished")