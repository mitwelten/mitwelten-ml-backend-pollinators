from flask import Flask
from flask import request

from .bucket import download_files, get_bucket_data, get_object_paths, get_client, extract_sub_prefix



client = get_client()
app = Flask(__name__)


# Load configurations from environment
# os.environ[]

@app.route('/get_objects/<bucket_name>/', methods=['GET'])
def get_objects(bucket_name):
    """_summary_

    Returns
    -------
    _type_
        _description_
    """
    pass


@app.route('/download/<bucket_name>/<object_name>/', methods=['GET'])
def download(bucket_name):
    """_summary_

    Returns
    -------
    _type_
        _description_
    """
    pass

@app.route('/upload/<bucket_name>/<filename>', methods=['PUT'])
def upload_data_s3(bucket_name, object_name, filename):
    """_summary_

    Returns
    -------
    _type_
        _description_
    """
    if request.method == 'PUT':
        pass
    else:
        raise Exception(f'Wrong method {request.method}') 




if __name__ == '__main__':  
    app.run(debug=False, port=8000, host='0.0.0.0')