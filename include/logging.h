#ifndef CS425_SDFS_LOGGING_H
#define CS425_SDFS_LOGGING_H

#include <string>
#include <queue>
#include <vector>
#include <sstream>
#include <fstream>
#include <iostream>
#include <map>
#include <mutex>
#include <shared_mutex>
#include <chrono>
#include <thread>
#include <iterator>
#include <algorithm>

namespace cs425::sdfs {

// Typedefs
using clk					= std::chrono::high_resolution_clock;
using timepnt				= std::chrono::time_point<clk>;
using duration_cast_nano	= decltype(std::chrono::duration_cast<std::chrono::nanoseconds>(clk::now() - clk::now()));
using duration_cast_micro	= decltype(std::chrono::duration_cast<std::chrono::microseconds>(clk::now() - clk::now()));
using duration_cast_milli	= decltype(std::chrono::duration_cast<std::chrono::milliseconds>(clk::now() - clk::now()));

class Logger {
friend class const_iterator;
public:
	// Public Classes
	class LogEvent;
	class const_iterator;

	class LogEvent {
	friend class Logger;
	public:
		// Constructors and the Rule of Four
		LogEvent();
		LogEvent(const LogEvent & other);
		LogEvent(LogEvent && other) noexcept;
		LogEvent & operator=(LogEvent other) noexcept;
		~LogEvent();

		// Public Methods
		/**
		 * Implements swapping functionality for the LogEvent class.
		 * Usage:
		 * 		using std::swap;	// Enable ADL
		 * 		swap(a, b);			// unqualified swap
		 *
		 * Arguments:
		 * 		a	- the first object whose data is to be swapped
		 * 		b	- the second object whose data is to be swapped
		 */
		friend void swap(LogEvent & a, LogEvent & b) noexcept;

		/**
		 * Prints the given log event to an output stream.
		 * Usage example:
		 * 		// Iterator must not be invalidated while printing
		 * 		const_iterator it = log.begin();
		 * 		std::cout << *it << std::endl;
		 */
		friend std::ostream & operator<<(std::ostream & strm, const LogEvent & evt);

		/**
		 * LessThanComparable
		 * Compares two LogEvents and returns true iff the first LogEvent
		 * (*this) comes before (<) or after (<) the second LogEvent
		 * (other).
		 *
		 * Arguments
		 * 		*this		- implicit. The first LogEvent to compare
		 * 		other		- the second LogEvent to compare
		 *
		 * Returns true iff (*this)'s timestamp is less than (<) or
		 * greater than (>) other's timestamp.
		 */
		bool operator<(const LogEvent & other) const;
		bool operator>(const LogEvent & other) const;
	
	private:
		// Private Constructors
		LogEvent(std::chrono::nanoseconds::rep ns_since_epoch, std::string driver, std::string log);

		// Private Fields
		std::chrono::nanoseconds::rep ns_since_epoch;
		std::string driver;
		std::string log;
	};

	class const_iterator {
	friend class Logger;
	public:
		using value_type = LogEvent;
		using difference_type = std::map<long long, std::string>::const_iterator::difference_type;
		using reference = const value_type &;
		using pointer = const value_type *;
		using iterator_category = std::bidirectional_iterator_tag;

		// Constructors and the Rule of Four
		const_iterator();
		const_iterator(const const_iterator & other);
		const_iterator(const_iterator && other) noexcept;
		const_iterator & operator=(const_iterator other) noexcept;
		~const_iterator();

		// Public Methods
		/**
		 * Implements swapping functionality for the const_iterator class.
		 * Usage:
		 * 		using std::swap;	// Enable ADL
		 * 		swap(a, b);			// unqualified swap
		 *
		 * Arguments:
		 * 		a	- the first object whose data is to be swapped
		 * 		b	- the second object whose data is to be swapped
		 */
		friend void swap(const_iterator & a, const_iterator & b) noexcept;

		/**
		 * Dereferenceable
		 * Dereferences the const_iterator.
		 *
		 * Returns a reference to the value of the const_iterator
		 */
		reference operator*() const;
		reference operator->() const;

		/**
		 * Incrementable
		 * Increments or decrements the const_iterator
		 *
		 * The prefix operators return a reference to the newly
		 * incremented const_iterator while the postfix operators return
		 * a copy of the previous value of const_iterator.
		 *
		 * An equivalent expression for the postfix increment operator is
		 * 		const_iterator j = i;
		 * 		++i;
		 * 		return j;
		 */
		const_iterator & operator++();
		const_iterator operator++(int);
		const_iterator & operator--();
		const_iterator operator--(int);

		/**
		 * EqualityComparable
		 * Compares two iterators.
		 *
		 * Given a, b, c of type const_iterator,
		 * 		- For all values of a, 'a == a' returns true
		 * 		- If 'a == b', then 'b == a'
		 * 		- If 'a == b' and 'b == c', then 'a == c'
		 * 		- 'a != b' == '!(a == b)'
		 *
		 * Multipass Guarantee
		 * Given a, b const_iterators and 'a == b'
		 * 		- Either both iterators are non-dereferenceable or *a
		 * 		  and *b are references bound to the same object
		 * 		- 'a == b' implies '++a == ++b'
		 *
		 * Returns a value indicating whether the expression is true.
		 */
		bool operator==(const const_iterator & other) const;
		bool operator!=(const const_iterator & other) const;

		// TODO: Implement LessThanComparable.
		// Useful? Good Design?

	private:
		// Private Constructors
		const_iterator(std::map<std::chrono::nanoseconds::rep, LogEvent>::const_iterator it);

		// Private fields
		std::map<std::chrono::nanoseconds::rep, LogEvent>::const_iterator map_it;
	};

	/**
	 * Constructors and the Rule of Four
	 *
	 * Default Constructor.
	 * Constructs the Logger with an empty name.
	 *
	 * Handle Constructor.
	 * Names the Logger. The name is used to print the driver of a given
	 * log event.
	 *
	 * Arguments:
	 * 		driver		- the name of the client using the Logger
	 */
	Logger();
	Logger(const std::string driver);
	Logger(const Logger & other) = delete;
	Logger & operator=(const Logger & other) = delete;
	Logger(Logger && other) noexcept;
	Logger & operator=(Logger && other) noexcept;
	~Logger() noexcept;

	// Factory Methods
	template <class... Ts>
	static Logger merge(const Ts & ... loggers, std::string driver = "")
		requires	(sizeof...(Ts) > 0) &&
					(std::is_same_v<Logger, std::common_type_t<Ts...>>);

	// TODO: Constrain for containers satisfying ForwardIteration
	template <class Container>
	static Logger merge(const Container & c, std::string driver = "");


	// Public Methods
	/**
	 * Implements swapping functionality for the Logger class.
	 * Usage:
	 * 		using std::swap;	// Enable ADL
	 * 		swap(a, b);			// unqualified swap
	 *
	 * Arguments:
	 * 		a	- the first object whose data is to be swapped
	 * 		b	- the second object whose data is to be swapped
	 */
	friend void swap(Logger & a, Logger & b) noexcept;

	/**
	 * Prints the given log event to an output stream.
	 * Usage example:
	 * 		// Iterator must not be invalidated while printing
	 * 		const_iterator it = log.begin();
	 * 		std::cout << *it << std::endl;
	 */
	friend std::ostream & operator<<(std::ostream & strm, const LogEvent & evt);


	/**
	 * Inserts tokens into the log.
	 * 		(1)	- the shift left (<<) operator appends the given
	 * 			  token to the log.
	 * 		(2)	- the shift left assign (<<=) operator appends the
	 * 			  given token to the log and finalizes the current
	 * 			  log entry. Inserting more tokens into the log
	 * 			  appends to the next log entry.
	 * 			  Invalidates all iterators.
	 *
	 * Multiple threads can use the same Logger object to append log
	 * events. Synchronization is achieved using locks, so the first
	 * (<<) blocks subsequent threads from continuing until the initial
	 * thread finalizes the entry with (<<=).
	 *
	 * Arguments:
	 * 		val	- the token to append
	 *
	 * Returns a reference to the Logger object.
	 */
	template <class T> Logger & operator<<(T val);
	template <class T> Logger & operator<<=(T val);

	/**
	 * Implements C++ Concepts: Lockable functionality for the Logger
	 * class.
	 *
	 * The recommended usage is to never call these functions directly.
	 * Instead, use an std::scoped_lock with Logger objects as arguments
	 * to lock multiple Logger objects simultaneously and release them
	 * when the std::scoped_lock goes out of scope.
	 *
	 * lock()		- blocks until the lock for the Logger object is
	 * 				  obtained.
	 * try_lock()	- tries to lock the Logger object, and returns
	 * 				  immediately regardless of success. If the lock was
	 * 				  acquired, the returned value is true, otherwise
	 * 				  false.
	 * unlock()		- releases the lock for the Logger object. Behavior
	 * 				  is undefined if the current thread does not have the
	 * 				  lock.
	 *
	 * TODO: std::scoped_lock for variable (run-time dependent) number
	 * of logs. Possible?
	 */
	void lock() const;
	bool try_lock() const;
	void unlock() const;

	/**
	 * Returns an iterator to events logged in this Logger
	 * begin()			- returns an iterator to the first LogEvent.
	 * end()			- returns a iterator to the element following
	 * 					  the last LogEvent.
	 * next(time)		- returns an iterator to the first LogEvent
	 * 					  logged after time.
	 *
	 * Must lock the log when manipulating iterators because all iterators
	 * are invalidated when a new log entry is added.
	 */
	const_iterator begin() const;
	const_iterator end() const;
	const_iterator next(std::chrono::nanoseconds::rep time) const;

	/**
	 * Returns the number of committed logs.
	 * Must lock the log before calling size()
	 */
	std::map<std::chrono::nanoseconds::rep, LogEvent>::size_type size() const;

	/**
	 * Writes the current log state to a file. The events written to
	 * the file are guaranteed to be in sorted order.
	 *
	 * This function should be called to finalize the log. The function
	 * only guarantees that the current log state is in sorted order - it
	 * does not make any guarantees for events committed to the log after
	 * the completion of this function.
	 * For example, if this function is executed concurrently with
	 * another thread logging its own events, uncommitted events that
	 * have been logged before the latest event in the current log state
	 * may not appear in the file output. Therefore, it is recommended
	 * to run this routine after all threads have finished logging.
	 *
	 * Arguments:
	 * 		filename		- the name of the file to write to
	 *
	 * Exceptions:
	 * 		runtime_error	- the file cannot be opened
	 */
	void write_to_file(std::string filename);

private:
	// Private Fields
	// Logging
	timepnt epoch;
	std::string driver;
	std::map<std::chrono::nanoseconds::rep, LogEvent> log_events;
	std::ostringstream log_strm;

	// Synchronization
	// TODO: Should synchronization be external to the Logger?
	mutable std::mutex log_mutex;
	std::thread::id current_thread;

};	// class Logger


// Templated Function Definitions
template <class... Ts>
Logger Logger::merge(const Ts & ... loggers, std::string driver)
	requires	(sizeof...(Ts) > 0) &&
				(std::is_same_v<Logger, std::common_type_t<Ts...>>) {
	
	Logger ret(driver);
	ret.epoch = *std::min_element({loggers.epoch...});
	(... , ret.log_events.insert(loggers.log_events.begin(), loggers.log_events.end()));
	return ret;
}

template <class Container>
Logger Logger::merge(const Container & c, std::string driver) {
	Logger ret(driver);

	std::vector<timepnt> epochs;
	epochs.reserve(c.size());

	for(auto && i : c) {
		epochs.push_back(i.epoch);
		ret.log_events.insert(i.log_events.begin(), i.log_events.end());
	}
	ret.epoch = *std::min_element(epochs.begin(), epochs.end());
	return ret;
}


template <class T>
Logger & Logger::operator<<(T val) {
	// Check if this thread already has the lock
	if(std::this_thread::get_id() != current_thread) {
		log_mutex.lock();
		current_thread = std::this_thread::get_id();
	}

	// Insert token
	log_strm << val;
	return *this;
}

template <class T>
Logger & Logger::operator<<=(T val) {
	// Insert token
	log_strm << val;

	// Get elapsed time
	std::chrono::nanoseconds::rep elapsed_time = duration_cast_nano(clk::now() - epoch).count();
	
	// Append log to storage
	auto && [it, b] = log_events.insert_or_assign(elapsed_time, LogEvent(elapsed_time, driver, log_strm.str()));

	/* Print log
	std::ostringstream oss;
	oss << it->second << std::endl;
	std::cout << oss.str();*/

	// No need to clear since we don't extract the data
	log_strm.str("");

	// Unlock the mutex so other threads can begin logging
	current_thread = std::thread::id();
	log_mutex.unlock();
	return *this;
}

/**
 * Contains definitions required for the const_merge functions
 */
namespace detail {
	using elem_t = std::pair<Logger::const_iterator, Logger::const_iterator>;
	auto mycomp = [](const elem_t & a, const elem_t & b) {
		return *(a.first) > *(b.first);
	};

	using return_type = std::vector<std::reference_wrapper<const Logger::const_iterator::value_type>>;
	using pq_type = std::priority_queue<elem_t, std::vector<elem_t>, decltype(mycomp)>;

	return_type const_merge_helper(pq_type & pq, return_type & ret) {
		while(!pq.empty()) {
			std::vector<elem_t>::const_reference min_elem = pq.top();
			ret.push_back(std::ref(*(min_elem.first)));

			if(Logger::const_iterator next_elem = min_elem.first; (++next_elem) != min_elem.second) {
				pq.push(std::pair(next_elem, min_elem.second));
			}
			pq.pop();
		}

		return ret;
	}

} // namespace cs425::sdfs::detail

/**
 * Merge multiple Loggers together in sorted order. This is useful in
 * order to read all logs in order of their time logged irrespective
 * of the entity doing the logging. The return format is a vector of
 * references to the LogEvents, in sorted order. Therefore, it is only
 * a temporary way to read LogEvents. Unlike Logger::merge, const_merge
 * does not construct a new Logger.
 *
 * Must not log events concurrently while executing this function
 * because it uses iterators to log events.
 *
 * Time Complexity
 * 		O(n log(k)), where n is the total number of log events in all
 * 		Loggers, and k is the number of Loggers.
 *
 * merge(loggers...)		- merge a number of Loggers known at compile
 * 							  time
 * merge(Container c)		- merge the Loggers stored in a Container
 * 							  that supports ForwardIteration.
 * Arguments
 * 		loggers...	- the Loggers to merge
 * 		c			- the container holding the Loggers to merge
 *
 * Returns a vector of references to the LogEvents in sorted order.
 */
template <class... Ts>
detail::return_type const_merge(const Ts & ... loggers)
	requires	(sizeof...(Ts) > 0) &&
				(std::is_same_v<Logger, std::common_type_t<Ts...>>) {

	detail::pq_type pq(detail::mycomp);
	detail::return_type ret;

	ret.reserve((... + loggers.size()));

	(... , pq.push(std::pair(loggers.begin(), loggers.end())));

	return detail::const_merge_helper(pq, ret);
}

// TODO: Constrain for containers satisfying ForwardIteration
template <class Container>
detail::return_type const_merge(const Container & c) {
	detail::pq_type pq(detail::mycomp);
	detail::return_type ret;

	int num_elems = 0;
	for(auto it = c.begin(); it != c.end(); it++) {
		num_elems += it->size();
		pq.push(std::pair(it->begin(), it->end()));
	}

	ret.reserve(num_elems);

	return detail::const_merge_helper(pq, ret);
}

}	// namespace cs425::sdfs

#endif	// CS425_SDFS_LOGGING_H
