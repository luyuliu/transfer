import run_transfer

from eve import Eve
app = Eve(settings='setting.py')

if __name__ == '__main__':
    app.run(port=5003)
