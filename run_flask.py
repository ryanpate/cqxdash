from app import app
import logging

# Setup logging
logging.basicConfig(
    filename='C:/inetpub/wwwroot/cqxdashboard/logs/flask_api.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s'
)

if __name__ == '__main__':
    print("Starting CQI Dashboard API on port 5000...")
    # Use threaded=True for handling multiple requests
    app.run(host='127.0.0.1', port=5000, debug=False, threaded=True)