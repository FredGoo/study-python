import sys
from http.server import HTTPServer, BaseHTTPRequestHandler
from time import sleep


class GetHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        # sleep(1)
        # sleep(164)
        self.send_response(400)
        message = "sb"
        # self.send_header('Content-Type', 'text/plain; charset=utf-8')
        self.end_headers()
        # self.wfile.write(message.encode('utf-8'))


def run(port):
    server_address = ('', int(port))
    httpd = HTTPServer(server_address, GetHandler)
    httpd.serve_forever()


if __name__ == "__main__":
    run(sys.argv[1])
