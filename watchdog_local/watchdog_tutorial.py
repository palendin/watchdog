
# # import the modules
# import sys
# import time
# import logging
# from watchdog.observers import Observer
# from watchdog.events import LoggingEventHandler
 
# watch for changes in the whole folder where the script lies
# if __name__ == "__main__":
#     # Set the format for logging info
#     logging.basicConfig(level=logging.INFO,
#                         format='%(asctime)s - %(message)s',
#                         datefmt='%Y-%m-%d %H:%M:%S')
 
#     # Set format for displaying path
#     path = sys.argv[1] if len(sys.argv) > 1 else '.'
 
#     # Initialize logging event handler
#     event_handler = LoggingEventHandler()
 
#     # Initialize Observer
#     observer = Observer()
#     observer.schedule(event_handler, path, recursive=True)
 
#     # Start the observer
#     observer.start()
#     try:
#         while True:
#             # Set the thread sleep time
#             time.sleep(1)
#     except KeyboardInterrupt:
#         observer.stop()
#     observer.join()


# import time module, Observer, FileSystemEventHandler
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import threading
 
# only watch for changes in file
class OnMyWatch:
    # Set the directory on watch
    watchDirectory = "C:\\Users\wayne\\.vscode\\API\\watchdog"
 
    def __init__(self):

        # Initialize Observer
        self.observer = Observer()
 
    def run(self):

        # creating own handler (see handler class below)
        event_handler = Handler()

        # Schedules watching a path and calls appropriate methods specified in the given event handler in response to file system events.
        self.observer.schedule(event_handler, self.watchDirectory, recursive = True)
        self.observer.start() # creates a new thread
        try:
            while True:
                time.sleep(5) # keeps main thread running
        except:
            self.observer.stop() # does some work before thread terminate upon receiving exception
            print("Observer Stopped")

        self.observer.join() # properly end a thread for "it blocks the thread in which you're making the call, until (self.observer) is finished." For reference: https://stackoverflow.com/questions/15085348/what-is-the-use-of-join-in-threading
        
 
class Handler(FileSystemEventHandler):
 
    @staticmethod
    def on_any_event(event):
        if event.is_directory:
            return None
 
        elif event.event_type == 'created':
            # Event is created, you can process it now
            print("Watchdog received created event - % s." % event.src_path)
        elif event.event_type == 'modified':
            # Event is modified, read the number of rows in the file
            print("Watchdog received modified event - % s." % event.src_path)
            with open('test.txt', 'r') as f:
                x = len(f.readlines())
                print(f'len is {x}')
        elif event.event_type == 'deleted':
            print('file is deleted - % s." % event.src_path')
            
                
             
 
if __name__ == '__main__':

    watch = OnMyWatch()
    watch.run()

    # while True:
    #     time.sleep(5)