//
//  pgclientlib.cpp
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

#include <sstream>
#include <iterator>

#include <boost/lexical_cast.hpp>

#include "pgclientlib.hpp"
#include "linenoise.hpp"

using namespace pgclientlib;

std::vector<std::string>
tokenize(const std::string& input)
{
    std::vector<std::string> res;
    std::istringstream ss(input);
    std::copy(std::istream_iterator<std::string>(ss),
              std::istream_iterator<std::string>(),
              std::back_inserter(res));
    return res;
}

std::string
get_par(const std::vector<std::string>& t,
        std::size_t pos = 0, std::string default_ = "")
{
    if (t.size() < pos + 1) return default_;
    if (t[pos].empty()) return default_;
    return t[pos];
}

void print_notifications(session& s)
{
    while (!s.notification_queue_empty())
        std::cout << s.get_notification() << std::endl;
}

void print_row(session& s)
{
    switch(s.get_buffer_format())
    {
        case session::buffer_format::query:
        {
            std::cout << "|";
            for (auto&& r : s.get_strings())
                std::cout << r << "|";
            std::cout << std::endl;
            break;
        }
        case session::buffer_format::copy_text:
        {
            auto r = s.get_raw_row();
            std::cout << std::string(r.begin(), r.end());
            break;
        }
        case session::buffer_format::copy_binary:
        {
            std::cout << s.get_strings().front();
            break;
        }
        default: std::cout << "Uknown buffer format" << std::endl;
    }
}

int main()
{
    linenoise::LoadHistory(".history");
    
    session s;
    int max_rows = 10;
    std::string prompt = "> ";
    while(true)
    {
        std::string line, input;
        while (line[0] != '\\' && line.find(";") == std::string::npos)
        {
            if(linenoise::Readline(prompt.c_str(), input)) return 0;
            if (input.empty()) line = "\\g"; else line += input;
        }
        linenoise::AddHistory(line.c_str());
        try
        {
            if (line[0] == '\\')
                switch(line[1])
                {
                    case 'c':
                    {
                        auto pars = tokenize(line);
                        std::string
                            port = get_par(pars, 1, "5432"),
                            path = get_par(pars, 2, "/private/tmp"),
                            prefix = get_par(pars, 3, ".s.PGSQL.");
                        s.connect_local(port, path, prefix);
                        std::cout << "Local connection on " << path << "/" << prefix << port << std::endl;
                        break;
                    }
                    case 'e':
                    {
                        s.toggle_echo_codes();
                        break;
                    }
                    case 'f':
                    {
                        auto fd = s.field_descriptors();
                        for (; fd.first != fd.second; ++fd.first)
                        {
                            std::cout << fd.first->first << '\t'
                                      << fd.first->second.table_oid << '\t'
                                      << fd.first->second.column_no << '\t'
                                      << fd.first->second.data_type << '\t'
                                      << fd.first->second.type_modf << '\t'
                                      << fd.first->second.frmt_code << std::endl;
                        }
                        break;
                    }
                    case 'g':
                    {
                        if (s.row_queue_empty())
                        {
                            std::cout << "No more rows pending" << std::endl;
                        }
                        else
                        {
                            for (int i = 0; i != max_rows; ++i)
                            {
                                if (s.row_queue_empty()) break;
                                print_row(s);
                            }
                        }
                        break;
                    }
                    case 'm':
                    {
                        auto pars = tokenize(line);
                        std::string mr = get_par(pars, 1, "10");
                        max_rows = boost::lexical_cast<int>(mr);
                        print_notifications(s);
                        break;
                    }
                    case 'p':
                    {
                        auto i = s.parameters();
                        for (; i.first != i.second; ++i.first)
                            std::cout << i.first->first << ": "
                                      << i.first->second << std::endl;
                        print_notifications(s);
                        break;
                    }
                    case 'q':
                    {
                        linenoise::SaveHistory(".history");
                        print_notifications(s);
                        s.terminate();
                        return 0;
                    }
                    case 'r':
                    {
                        s.clear_row_queue();
                        break;
                    }
                    case 's':
                    {
                        auto pars = tokenize(line);
                        std::string
                            user = get_par(pars, 2, getlogin()),
                            database = get_par(pars, 1, "");
                        s.startup(user, database);
                        if (database.empty()) database = user;
                        std::cout << "Connected to " << database
                                  << " as user " << user << std::endl;
                        prompt = database + "> ";
                        print_notifications(s);
                        break;
                    }
                    case 't':
                    {
                        auto pars = tokenize(line);
                        std::string
                            host = get_par(pars, 1, "localhost"),
                            service = get_par(pars, 2, "postgresql");
                        s.connect_tcp(host, service);
                        print_notifications(s);
                        std::cout << "TCP connection to " << host
                                  << " on service or port " << service << std::endl;
                        break;
                    }
                    case 'z':
                    {
                        s.cancel();
                        break;
                    }
                    default: std::cout << "Unrecognized command" << std::endl;
                }
            else
            {
                s.query(line);
                print_notifications(s);
            }
        }
        catch(const std::runtime_error& e)
        {
            std::cout << "Caught exception: " << e.what() << std::endl;
        }
        catch(...)
        {
            std::cout << "Caught unhandled exception" << std::endl;
            return 1;
        }
    }
    s.terminate();
    linenoise::SaveHistory(".history");
    return 0;
}
