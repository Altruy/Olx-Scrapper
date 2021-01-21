from flask import Flask, render_template, send_file, abort
import os

app = Flask(__name__)

@app.route('/')
def index():
    crawls = [i for i in os.listdir('crawls') if i.endswith('.csv')]
    return render_template('index.html', crawls=crawls)


@app.route('/download/<filename>')
def download(filename):
    filepath = os.path.join('crawls', filename)
    if not os.path.exists(filepath):
        abort(404)
        
    return send_file(filepath)
        
        
if __name__ == "__main__":
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
