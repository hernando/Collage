
/* Copyright (c) 2005-2006, Stefan Eilemann <eile@equalizergraphics.com> 
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *  
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */

#include <test.h>
#include <eq/eq.h>

#include <iostream>

using namespace eq::base;
using namespace eq::net;
using namespace std;

RefPtr<eq::net::Connection> connection;

class NodeThread : public eq::base::Thread
{
protected:
    virtual void* run()
        {
            RefPtr<Node>        node      = new Node;
            RefPtr<eq::net::Node> nodeProxy = new eq::net::Node;

            TEST( node->listen( ));

            eq::net::PipeConnection* pipeConnection = 
                (eq::net::PipeConnection*)connection.get();

            TEST( node->connect( nodeProxy, pipeConnection->getChildEnd( )));
            TEST( nodeProxy->isConnected( ));
            
            Session session;
            TEST( node->mapSession( nodeProxy, &session, "foo" ));

            TEST( node->stopListening( ));
            return EXIT_SUCCESS;
        }
};

int main( int argc, char **argv )
{
    eq::net::init( argc, argv );

    connection = new PipeConnection();
    TEST( connection->connect( ));

    NodeThread thread;
    thread.start();

    RefPtr<Node> node      = new Node;
    RefPtr<Node> nodeProxy = new Node;

    TEST( node->listen( ));
    TEST( node->connect( nodeProxy, connection ));
    TEST( nodeProxy->isConnected( ));

    Session session;
    TEST( node->mapSession( node, &session, "foo" ));
    
    TEST( node->stopListening( ));
    thread.join();
    node = NULL;
    nodeProxy = NULL;
}

