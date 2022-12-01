import sys
import time

import torch.cuda as cuda
import pandas as pd


if __name__ == '__main__':
    for i in range(6):
        sys.stdout.write('Checking for CUDA devices ' + i*'.' + '\r')
        time.sleep(.3)

    print('Checking for CUDA devices ...')
    
    print(f'CUDA Device available: [{cuda.is_available()}]')
    if cuda.is_available():
        df = None
        for i in range(cuda.device_count()):    
            device = cuda.get_device_properties(i)
            df_ = pd.DataFrame(data={
                'Name': [device.name],
                'Major': [device.major],
                'Minor': [device.minor],
                'Memory (MB)': [device.total_memory / (1000 * 1000)],
                'Multi Processor Count': [device.multi_processor_count]
            })
            if df is None:
                df = df_.copy()
            else:
                df = pd.concat([df, df_], axis=0)
        
        print(df.to_markdown())
        