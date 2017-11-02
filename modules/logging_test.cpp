#include <iostream>
#include <thread>

#include "logging.h"

#define NUM_THREADS		10
#define NUM_LOGS		100
#define NUM_READS		100

using namespace cs425::sdfs;
using namespace std::literals::chrono_literals;

void make_logs(Logger & log) {
	for(int i = 0; i < NUM_LOGS; i++) {
		log << "Log #" <<= i;
		std::this_thread::sleep_for(100us);
	}
}

void read_logs(const std::array<Logger, NUM_THREADS> & loggers) {
	int num_printed = 0;
	Logger::LogEvent last_log;

	int i = 0;
	do {
		// Hello? deadlock!
		// Hi, each thread only locks one mutex. No deadlock possible.
		for(auto && j : loggers)
			j.lock();
		
		auto events = const_merge(loggers);

		if(auto it = std::upper_bound(events.begin(), events.end(), last_log); it != events.end()) {
			std::cout << "\n\nRead #" << i << "\n" << std::endl;
			for(; it != events.end(); it++) {
				std::cout << "Here: " << *it << std::endl;
				num_printed++;
			}

			last_log = events.back();
		} else if(i < NUM_READS) {
			std::this_thread::yield();
		} else {
			for(auto && j : loggers)
				j.unlock();

			break;
		}

		for(auto && j : loggers)
			j.unlock();

		i++;
	} while(true);

	std::cout << "\n\nNum Printed: " << num_printed << std::endl;
}

int main(int argc, char ** argv) {
	// Instantiate Loggers
	std::array<Logger, NUM_THREADS> loggers;
	for(int i = 0; i < NUM_THREADS; i++) {
		loggers[i] = Logger("Thread " + std::to_string(i));
	}

	// Write to *different* Logger objects from each thread
	// Theoretical synchronization overhead: 0
	// Read from all Logger objects: need to lock all Loggers
	{
		std::array<std::thread, NUM_THREADS> write_threads;
		for(int i = 0; i < NUM_THREADS; i++) {
			write_threads[i] = std::thread(make_logs, std::ref(loggers[i]));
		}

		std::thread read_thread(read_logs, std::ref(loggers));

		for(int i = 0; i < NUM_THREADS; i++) {
			write_threads[i].join();
		}
		read_thread.join();
	}

	// Write to file
	Logger::merge(loggers).write_to_file("out/log.txt");

	return 0;
}
