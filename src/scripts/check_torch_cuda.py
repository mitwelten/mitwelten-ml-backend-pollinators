import torch.cuda as cuda
import pandas as pd

if __name__ == '__main__':
    print('Checking for CUDA devices ...')
    print(f'CUDA Device available: [{cuda.is_available()}]')
    if cuda.is_available():
        device = cuda.get_device_properties(0)
        df = pd.DataFrame(data={
            'Name': [device.name],
            'Major': [device.major],
            'Minor': [device.minor],
            'Memory': [device.total_memory / (1000 * 1000)],
            'Multi Processor Count': [device.multi_processor_count]
        })
        print(df.to_markdown())
        