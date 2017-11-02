#include "logging.h"

#include <iomanip>

#define PRECISION			6
#define CLOCK_WIDTH			12
#define HANDLE_WIDTH		16

namespace cs425::sdfs {

//////////////////////////////////////////////////
//////////           LogEvent           //////////
//////////////////////////////////////////////////

// Constructors and the Rule of Four
Logger::LogEvent::LogEvent()
	: ns_since_epoch(0),
	  driver(""),
	  log("") {
}

Logger::LogEvent::LogEvent(std::chrono::nanoseconds::rep ns_since_epoch, std::string driver, std::string log)
	: ns_since_epoch(ns_since_epoch),
	  driver(driver),
	  log(log) {
}

Logger::LogEvent::LogEvent(const LogEvent & other)
	: ns_since_epoch(other.ns_since_epoch),
	  driver(other.driver),
	  log(other.log) {
}

Logger::LogEvent::LogEvent(LogEvent && other) noexcept {
	swap(*this, other);
}

Logger::LogEvent & Logger::LogEvent::operator=(LogEvent other) noexcept {
	swap(*this, other);
	return *this;
}

Logger::LogEvent::~LogEvent() {
	// Nothing
}


// Public Methods
void swap(Logger::LogEvent & a, Logger::LogEvent & b) noexcept {
	using std::swap;

	swap(a.ns_since_epoch, b.ns_since_epoch);
	swap(a.driver, b.driver);
	swap(a.log, b.log);
}

std::ostream & operator<<(std::ostream & strm, const Logger::LogEvent & evt) {
	// TODO: Terminal Colors
	return strm << "[" << std::setprecision(PRECISION) << std::fixed << std::right << std::setw(CLOCK_WIDTH) << (double)evt.ns_since_epoch / 1000000000 << "] " << std::setw(HANDLE_WIDTH) << std::left << evt.driver << ": " << evt.log;
}

bool Logger::LogEvent::operator<(const LogEvent & other) const {
	return ns_since_epoch < other.ns_since_epoch;
}

bool Logger::LogEvent::operator>(const LogEvent & other) const {
	return ns_since_epoch > other.ns_since_epoch;
}


//////////////////////////////////////////////////
//////////        const_iterator        //////////
//////////////////////////////////////////////////

// Constructors and the Rule of Four
Logger::const_iterator::const_iterator() {}

Logger::const_iterator::const_iterator(std::map<std::chrono::nanoseconds::rep, LogEvent>::const_iterator it) : map_it(it) {}

Logger::const_iterator::const_iterator(const const_iterator & other) : map_it(other.map_it) {}

Logger::const_iterator::const_iterator(const_iterator && other) noexcept {
	swap(*this, other);
}

Logger::const_iterator & Logger::const_iterator::operator=(const_iterator other) noexcept {
	swap(*this, other);
	return *this;
}

Logger::const_iterator::~const_iterator() {
	// Nothing
}


// Public Methods
void swap(Logger::const_iterator & a, Logger::const_iterator & b) noexcept {
	using std::swap;
	swap(a.map_it, b.map_it);
}

Logger::const_iterator::reference Logger::const_iterator::operator*() const {
	return map_it->second;
}

Logger::const_iterator::reference Logger::const_iterator::operator->() const {
	return map_it->second;
}

Logger::const_iterator & Logger::const_iterator::operator++() {
	++map_it;
	return *this;
}

Logger::const_iterator Logger::const_iterator::operator++(int) {
	const_iterator j = *this;
	++map_it;
	return j;
}

Logger::const_iterator & Logger::const_iterator::operator--() {
	--map_it;
	return *this;
}

Logger::const_iterator Logger::const_iterator::operator--(int) {
	const_iterator j = *this;
	--map_it;
	return j;
}

bool Logger::const_iterator::operator==(const const_iterator & other) const {
	return map_it == other.map_it;
}

bool Logger::const_iterator::operator!=(const const_iterator & other) const {
	return map_it != other.map_it;
}


//////////////////////////////////////////////////
//////////            Logger            //////////
//////////////////////////////////////////////////

// Constructors and the Rule of Four
Logger::Logger() : epoch(clk::now()), driver("") {}

Logger::Logger(std::string driver) : epoch(clk::now()), driver(driver) {}

Logger::Logger(Logger && other) noexcept {
	swap(*this, other);
}

Logger & Logger::operator=(Logger && other) noexcept {
	swap(*this, other);
	return *this;
}

Logger::~Logger() {
	// Nothing
}


// Public Methods
void swap(Logger & a, Logger & b) noexcept {
	// Get exclusive locks
	std::scoped_lock lock(a.log_mutex, b.log_mutex);

	using std::swap;

	// Logging
	swap(a.epoch, b.epoch);
	swap(a.driver, b.driver);
	swap(a.log_events, b.log_events);
	swap(a.log_strm, b.log_strm);

	// Synchronization
	swap(a.current_thread, b.current_thread);
}

// Lockable functionality
void Logger::lock() const { log_mutex.lock(); }
bool Logger::try_lock() const { return log_mutex.try_lock(); }
void Logger::unlock() const { log_mutex.unlock(); }

// Iterator functions
Logger::const_iterator Logger::begin() const {
	return const_iterator(log_events.begin());
}

Logger::const_iterator Logger::end() const {
	return const_iterator(log_events.end());
}

Logger::const_iterator Logger::next(std::chrono::nanoseconds::rep time) const {
	return const_iterator(log_events.upper_bound(time));
}

std::map<long long, std::string>::size_type Logger::size() const {
	return log_events.size();
}

void Logger::write_to_file(std::string filename) {
	// File is closed when the stream goes out of scope
	std::ofstream ofs(filename);

	// Try to open the file
	if(!ofs.is_open())
		throw std::runtime_error("Cannot open file");
	
	// Print events to file
	for(auto && [k, v] : log_events) {
		ofs << v << std::endl;
	}
}

}	// namespace cs425::sdfs
