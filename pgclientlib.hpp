//
//  pgclientlib.hpp
//  pgclientlib
//
//  Created by Tim Keitt on 5/8/17.
//  Copyright © 2017 Tim Keitt. All rights reserved.
//
// MIT License
//
// Copyright (c) 2017 Timothy H. Keitt
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//

#ifndef pgclientlib_hpp
#define pgclientlib_hpp

#include <stdio.h>
#include <iostream>
#include <vector>
#include <queue>
#include <unordered_map>
#include <boost/endian/arithmetic.hpp>
#include <asio.hpp>

/**
 * Client session class. Represents a client session. Manages all state and communications with the server.
 */
class session
{
public:
    using buffer_type = std::vector<std::uint8_t>; /**< Raw buffer type. */
    using parameter_map = std::unordered_map<std::string, std::string>; /**< Container for parameters. */
    using parameter_map_iter_type = parameter_map::const_iterator; /**< Iterator over session parameters. */
    using parameter_map_value_type = parameter_map::value_type; /**< Type returned by dereferencing parameter_map_iter_type. */
    using raw_row_type = buffer_type; /**< Container of data bytes returned from server, absent the header (code and size). */
    using row_type = std::vector<std::string>; /**< Container of strings returned from server. */

    /**
     * Represents server transaction status.
     */
    enum struct transaction_status
    {
        idle,   /**< Not in tracation        */
        active, /**< In transaction          */
        error   /**< Transaction error state */
    };
    
    /**
     * Information about record fields.
     */
    struct field_descriptor
    {
        boost::endian::big_int32_t table_oid; /**< Oid of table or zero */
        boost::endian::big_int16_t column_no; /**< Column number or zero */
        boost::endian::big_int32_t data_type; /**< Oid of the column type */
        boost::endian::big_int16_t type_size; /**< Binary type width (negative is variable width) */
        boost::endian::big_int32_t type_modf; /**< A type-specific flag */
        boost::endian::big_int16_t frmt_code; /**< Zero for text and one for binary */
    };
    
    using field_map_type = std::vector<std::pair<std::string, field_descriptor>>; /**< Container for field descriptors. */
    using field_map_iter_type = field_map_type::const_iterator; /**< Iterator over field descriptors. */
    using field_map_value_type = field_map_type::value_type; /**< Value returned when dereferencing iterator. */

    session() : socket(io_service) {}
    
    session(const session&) = delete;
    session& operator=(const session&) = delete;
    
    /**
     * Connect over domain socket. Connects to a server running on the local machine.
     * The full path is assembled as path + "/" + prefix + port.
     *
     * Throws std::runtime_error
     * on failure to open the socket.
     * \param port The port number. Appended to the socket file path.
     * \param path The location of the domain socket file.
     * \param prefix The socket file name absent the port.
     */
    void connect_local(const std::string port = "5432",
                       const std::string path = "/private/tmp",
                       const std::string prefix = ".s.PGSQL.")
    {
        if (socket.is_open()) socket.close();
        std::string ep = path + "/" + prefix + port;
        asio::local::stream_protocol::endpoint endpoint(ep);
        socket.connect(endpoint);
        if (!socket.is_open())
            throw std::runtime_error("Could not open socket");
    }
    
    /**
     * Connect over TCP socket. Connects to a server on the specified host and service.
     *
     * Throws std::runtime_error on failure to open the socket.
     * \param host The hostname or IP address.
     * \param service The service name or port number.
     */
    void connect_tcp(const std::string host = "localhost",
                     const std::string service = "postgresql")
    {
        if (socket.is_open()) socket.close();
        asio::ip::tcp::resolver resolver(io_service);
        asio::ip::tcp::resolver::query query(host, service);
        auto endpoint_iterator = resolver.resolve(query);
        auto end = asio::ip::tcp::resolver::iterator();
        while (endpoint_iterator != end)
        {
            asio::error_code ec;
            auto ep = endpoint_iterator->endpoint();
            socket.connect(ep, ec);
            if (!ec) break;
            ++endpoint_iterator;
        }
        if (endpoint_iterator == end)
            throw std::runtime_error("Could not open socket");
    }
    
    /**
     * Initiate dialog with server. Sends a startup message. All session parameters are reset.
     * All replies are processed until the server is ready to accept input or an error is returned.
     *
     * Returns true if server is ready to accept input.
     *
     * Throws (from asio) if unable to communicate with server.
     * \param user The database role name.
     * \param database The database name (defaults to user).
     */
    bool startup(const std::string user, const std::string database = "")
    {
        pars.clear();
        send_msg(startup_msg(user, database));
        process_reply(get_reply());
        return ready();
    }
    
    bool is_ready_for_input() const { return ready();          } /**< Check if session is ready to accept input. */
    bool socket_is_open()     const { return socket.is_open(); } /**< Check if transport socket is open. */
    
    void terminate() { send_msg({'X', 0, 0, 0, 4}); } /**< Send the terminate message. */
    void sync()      { send_msg({'S', 0, 0, 0, 4}); } /**< Send the sync message. */
    void flush()     { send_msg({'H', 0, 0, 0, 4}); } /**< Send the flush message. */
    void cancel()    { send_msg(  cancel_msg()   ); } /**< Send a cancel message (might be ignored). */
    
    /**
     * Transmit a message. All replies will be processed until the server is
     * again ready for input or data were returned. If data are returned, only
     * a single data row will be enqueued. It is up to the client to requrest the
     * remaining data rows to be processed. You can call "while(enqueue_row()){;}" to
     * enque all remaining rows, or handle them one-at-a-time. Note that enqueued data
     * is always retained until a subsequent query returns new data, or reset_data is
     * true. There is intentionally no function that returns all data at once.
     *
     * Returns true if data are returned or the server is otherwise not ready.
     * Check the row queue to see if data rows are pending.
     *
     * \param request The query string.
     * \param reset_data If true, process all pending server messages without enqueuing data
     *                    and clear the row queue.
     */
    bool query(const std::string& request, bool reset_data = false)
    {
        if (reset_data)
        {
            clear_row_queue();
            discard_pending_data();
        }
        send_msg(query_msg(request));
        process_until_data();
        return not_ready();
    }
    
    /**
     * Return row as strings. Splits the raw buffer into fields and returns thme
     * as a vector of strings. Binary output may cause issues with string encoding.
     *
     * \param dequeue If true, remove the row from the row queue.
     */
    row_type
    get_row(bool dequeue = true)
    {
        row_type res;
        if (row_queue_empty()) return res;
        auto rr = front_raw_row();
        boost::endian::big_int16_t n;
        std::memcpy(&n, &rr[0], 2);
        auto i = &rr[2];
        for (int j = 0; j != n; ++j)
        {
            boost::endian::big_int32_t sz;
            std::memcpy(&sz, i, 4); i += 4;
            assert(j < field_map.size());
            if (field_map[j].second.frmt_code)
            {
                res.emplace_back("<binary>");
                continue;
            }
            if (sz == -1)
            {
                res.emplace_back();
                continue;
            }
            res.emplace_back(i, i + sz);
            i += sz;
        }
        if (dequeue) pop_row_queue();
        return res;
    }
    
    /**
     * Retrieve raw byte stream from row queue.
     */
    raw_row_type front_raw_row() const
    {
        return row_queue.front();
    }

    /**
     * Fetch a row from the server.
     */
    bool enqueue_row()
    {
        process_until_data();
        if (ready()) return false;
        return true;
    }
    
    /**
     * Remove and return a raw row.
     */
    raw_row_type dequeue_raw_row()
    {
        if (row_queue.empty()) throw
            std::runtime_error("Attempt to pop empty row queue");
        auto row = row_queue.front();
        row_queue.pop();
        return row;
    }
    
    /**
     * Remove a row from the queue.
     */
    bool pop_row_queue()
    {
        if (row_queue.empty()) throw
            std::runtime_error("Attempt to pop empty row queue");
        row_queue.pop();
        return !row_queue.empty();
    }

    /**
     * Remove all rows from the row queue.
     */
    void clear_row_queue()
    {
        while(row_queue_full()) pop_row_queue();
    }

    bool row_queue_full()        const { return !row_queue.empty(); } /**< True if rows in queue. */
    bool row_queue_empty()       const { return row_queue.empty();  } /**< False if rows in queue. */
    std::size_t row_queue_size() const { return row_queue.size();   } /**< Number of rows in queue. */
    
    /**
     * Remove and return a notification string.
     */
    std::string dequeue_notification()
    {
        if (notifications.empty()) throw
            std::runtime_error("Attempt to pop empty notification queue");
        auto msg = notifications.front();
        notifications.pop();
        return msg;
    }
    
    /**
     * Remove a notification string from the queue.
     */
    bool pop_notification()
    {
        if (notifications.empty()) throw
            std::runtime_error("Attempt to pop empty notification queue");
        notifications.pop();
        return !row_queue.empty();
    }
    
    bool notification_queue_full()        const { return !notifications.empty(); } /**< True if notifications in queue. */
    bool notification_queue_empty()       const { return notifications.empty();  } /**< False if notifications in queue. */
    std::size_t notification_queue_size() const { return notifications.size();   } /**< Number of notifications in queue. */

    /**
     * Retrieve parameter value. Session parameters are stored in a map of key-value pairs.
     * This function returns a pair whose first member is the parameter value and whose
     * second member is a boolean indicating weather the parameter was set.
     */
    std::pair<std::string, bool>
    get_parameter(const std::string& key)
    {
        return std::make_pair(pars[key], pars.find(key) != pars.end());
    }
    
    /**
     * Retrive an iterator pair over session parameters.
     */
    std::pair<parameter_map_iter_type, parameter_map_iter_type>
    parameters() const
    {
        return std::make_pair(pars.begin(), pars.end());
    }
    
    /**
     * Return iterator to field descriptors. Returns an iterator-pair marking the beginning and end
     * of the field descriptors. A field descriptor is a pair whose first member is the column name
     * and whose second member is a field_descriptor struct.
     */
    std::pair<field_map_iter_type, field_map_iter_type>
    field_descriptors() const
    {
        return std::make_pair(field_map.begin(), field_map.end());
    }
    
    /**
     * Return transaction status enum.
     */
    transaction_status
    get_transaction_status() const
    {
        return ts;
    }
    
    ~session()
    {
        try { cleanup(); }
        catch(...)
        {
            std::cout << "Caught exception in session destructor" << std::endl;
        }
    }
    
private:
    void cleanup()
    {
        if (socket.is_open())
        {
            terminate();
            socket.close();
        }
    }
    
    void process_until_data()
    {
        while (not_ready())
        {
            auto msg = get_reply();
            process_reply(msg);
            if (is_data(msg)) break;
        }
    }
    
    void discard_pending_data()
    {
        while (not_ready())
            discard_data(get_reply());
    }

    struct server_message_header
    {
        using code_type = std::uint8_t;
        using length_type = boost::endian::big_int32_t;
        using size_type = std::size_t;
        code_type code;
        length_type length;
        size_type unread_bytes() const
        {
            return length - sizeof(length);
        }
    };
    
    void send_msg(const buffer_type& msg)
    {
        ready_for_query = false;
        asio::write(socket, asio::buffer(msg));
    }
    
    bool not_ready() const { return ready_for_query == false; }
    bool ready()     const { return ready_for_query == true;  }
    
    server_message_header
    get_reply()
    {
        server_message_header reply;
        asio::read(socket, asio::buffer(&reply, sizeof(reply)));
        return reply;
    }
    
    void
    append(buffer_type& buf,
           const std::string& msg,
           unsigned int nulls = 1) const
    {
        buf.insert(buf.end(), msg.begin(), msg.end());
        while (nulls--) buf.push_back({});
    }
    
    buffer_type
    startup_msg(const std::string user, std::string database) const
    {
        buffer_type msg(8, 0); msg[5] = 3;
        if (database.empty()) database = user;
        append(msg, "user"); append(msg, user);
        append(msg, "database"); append(msg, database, 2);
        boost::endian::big_int32_t len = msg.size();
        std::memcpy(&msg[0], &len, sizeof(len));
        return msg;
    }
    
    buffer_type
    cancel_msg() const
    {
        buffer_type msg = {  0,  0,  0, 16, 80, 87, 71, 02,
                             0,  0,  0,  0,  0,  0,  0,  0 };
        std::memcpy(&msg[8], &pid, 4);
        std::memcpy(&msg[12], &skey, 4);
        return msg;
    }
    
    buffer_type
    query_msg(const std::string& request) const 
    {
        buffer_type msg = { 'Q', 0, 0, 0, 0 };
        boost::endian::big_int32_t len = request.size() + 5;
        std::memcpy(&msg[1], &len, sizeof(len));
        append(msg, request);
        return msg;
    }

    bool is_error(const server_message_header& msg) { return msg.code == 'E'; }
    bool is_ready(const server_message_header& msg) { return msg.code == 'Z'; }
    bool is_data(const server_message_header& msg) { return msg.code == 'D'; }
    
    void discard_data(const server_message_header& msg)
    {
        switch(msg.code)
        {
            case 'D':
            case 'T':
            {
                read_remaining(msg);
                break;
            }
            default: process_reply(msg);
        }
    }
    
    void process_reply(const server_message_header& msg)
    {
        switch(msg.code)
        {
            case 'A': // NotificationResponse
            {
                auto buf = read_remaining(msg);
                parse_notifications(buf);
                break;
            }
            case 'C': // CommandComplete
            {
                auto buf = read_remaining(msg);
                notifications.push(buf2str(buf));
                break;
            }
            case 'D': // DataRow
            {
                row_queue.push(read_remaining(msg));
                break;
            }
            case 'E': // ErrorResponse
            {
                auto buf = read_remaining(msg);
                parse_notifications(buf);
                break;
            }
            case 'I': // EmptyQuery
            {
                read_remaining(msg);
                notifications.push("[Empty request]");
                break;
            }
            case 'K': // BackendKeyData
            {
                assert(msg.unread_bytes() == 8);
                pid = read<boost::endian::big_int32_t>();
                skey = read<boost::endian::big_int32_t>();
                break;
            }
            case 'N': // NoticeResponse
            {
                auto buf = read_remaining(msg);
                parse_notifications(buf);
                break;
            }
            case 'R': // Authentication
            {
                assert(msg.unread_bytes() == 4);
                auto auth_code = read<boost::endian::big_int32_t>();
                if (auth_code)
                    throw std::runtime_error("Autentication mode not supported");
                auto auth_msg = get_reply();
                process_reply(auth_msg);
                if (is_error(auth_msg))
                {
                    throw std::runtime_error("Error in startup; cannot continue");
                }
                while (not_ready()) process_reply(get_reply());
                break;
            }
            case 'S': // ParameterStatus
            {
                parse_params(read_remaining(msg));
                break;
            }
            case 'T': // RowDescription
            {
                field_map.clear();
                auto buf = read_remaining(msg);
                boost::endian::big_int16_t nfields;
                std::memcpy(&nfields, &buf[0], 2);
                auto start_pos = buf.begin() + 2;
                while (nfields--)
                {
                    field_descriptor fd;
                    auto first_null = std::find(start_pos, buf.end(), '\0');
                    std::string field_name(start_pos, first_null);
                    std::memcpy(&fd, &*first_null + 1, sizeof(fd));
                    field_map.push_back(std::make_pair(field_name, fd));
                    start_pos = first_null + sizeof(fd) + 1;
                }
                while (!row_queue.empty()) row_queue.pop();
                break;
            }
            case 'Z': // ReadyForQuery
            {
                switch (read<std::uint8_t>())
                {
                    case 'I': ts = transaction_status::idle; break;
                    case 'T': ts = transaction_status::active; break;
                    case 'E': ts = transaction_status::error; break;
                    default: throw std::runtime_error("Invalid transaction status");
                }
                ready_for_query = true;
                break;
            }
            default:
            {
                read_remaining(msg);
                throw std::runtime_error("Cannot handle server response");
                break;
            }
        } // switch msg.code
    } // process reply
    
    template <typename T>
    T read()
    {
        T res;
        asio::read(socket, asio::buffer(&res, sizeof(res)));
        return res;
    }
    
    buffer_type
    read_remaining(const server_message_header& msg)
    {
        buffer_type buf(msg.unread_bytes());
        asio::read(socket, asio::buffer(buf));
        // debug_msg(buf);
        return buf;
    }

    void parse_notifications(const buffer_type& buf)
    {
        std::stringstream ss;
        auto
            i = std::begin(buf),
            e = std::end(buf);
        while (*i && i != e)
        {
            switch (*i)
            {
                case 'M':
                {
                    ss << ": ";
                    while (*++i && i != e) ss << *i;
                    break;
                }
                case 'S':
                {
                    while (*++i && i != e) ss << *i;
                    break;
                }
                default: while (*++i && i != e);
            }
            if (i != e) ++i;
        }
        notifications.push(ss.str());
    }
    
    void parse_params(const buffer_type& buf)
    {
        std::string key, value;
        auto i = buf.begin();
        while (*i) key.push_back(*i++);
        while (*++i) value.push_back(*i);
        pars[key] = value;
    }
    
    std::string buf2str(const buffer_type& msg)
    {
        return std::string(msg.begin(), msg.end());
    }
    
    void debug_msg(const buffer_type& msg) const
    {
        for (int i = 0; i != msg.size(); ++i)
            std::cout << msg[i]; std::cout << std::endl;
        for (int i = 0; i != msg.size(); ++i)
            std::cout << std::hex << (msg[i] >> 4); std::cout << std::endl;
        for (int i = 0; i != msg.size(); ++i)
            std::cout << std::hex << (msg[i] & 0xF); std::cout << std::endl;
    }
    
    asio::io_service io_service;
    asio::generic::stream_protocol::socket socket;
    transaction_status ts = transaction_status::idle;
    boost::endian::big_int32_t pid = 0, skey = 0;
    std::queue<std::string> notifications = {};
    std::queue<buffer_type> row_queue = {};
    field_map_type field_map = {};
    bool ready_for_query = false;
    parameter_map pars = {};
};

#endif /* pgclientlib_hpp */
