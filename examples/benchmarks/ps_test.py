
import dtw


@dtw.remote
class Worker:
    import torch.nn as nn
    import torch.nn.functional as F
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
    
    def __init__(self,worker_id, num_workers, lr=0.1, batch_size=128):
        import torch
        import torch.nn as nn
        from torchvision import datasets, transforms
        from torch.utils.data import DataLoader, Subset

        self.worker_id = worker_id
        self.num_workers = num_workers

        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.model = self.Net().to(self.device)
        self.optimizer = torch.optim.SGD(self.model.parameters(), lr=lr)
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

        indices = list(range(worker_id, len(dataset), num_workers))
        subset = Subset(dataset, indices)

        self.loader = DataLoader(subset, batch_size=batch_size, shuffle=True)
        self.iterator = iter(self.loader)

    def compute_gradients(self):
        self.model.train()

        try:
            x, y = next(self.iterator)
        except StopIteration:
            self.iterator = iter(self.loader)
            x, y = next(self.iterator)

        x, y = x.to(self.device), y.to(self.device)

        self.optimizer.zero_grad()
        out = self.model(x)
        loss = self.loss_fn(out, y)
        loss.backward()

        grads = {
            name: param.grad.detach().cpu()
            for name, param in self.model.named_parameters()
        }
        return grads, loss.item()
    
    def apply_gradients(self, avg_grads):
        for name, param in self.model.named_parameters():
            param.grad = avg_grads[name].to(self.device)
        self.optimizer.step()
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

    workers = [
        Worker.party("dtwroute").remote(i, num_workers,0.1, 128) for i in range(num_workers)
    ]

    for step in range(5):
        results = [
            w.compute_gradients.remote()
            for w in workers
        ]
        results = [dtw.get(obj) for obj in results]

        grads_list, losses = zip(*results)
        avg_grads = ps.average(list(grads_list))

        was = [w.apply_gradients.remote(avg_grads) for w in workers]
        was = [dtw.get(wa) for wa in was]
        
        print(f"Step {step}, loss: {sum(losses)/len(losses):.4f}")

    
    for w in workers:
        w.free()
    
    print("Training finished")




    