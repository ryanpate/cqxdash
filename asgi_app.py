# asgi_app.py
from app import app
from asgiref.wsgi import WsgiToAsgi
asgi_app = WsgiToAsgi(app)