
/* Copyright (c) 2005-2012, Stefan Eilemann <eile@equalizergraphics.com>
 *                    2012, Daniel Nachbaur <danielnachbaur@gmail.com>
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License version 2.1 as published
 * by the Free Software Foundation.
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

#include <co/connection.h>
#include <co/connectionDescription.h>
#include <co/iCommand.h>
#include <co/init.h>
#include <co/node.h>
#include <co/oCommand.h>
#ifdef COLLAGE_USE_MPI
#  include <co/mpi.h>
#endif

#include <lunchbox/clock.h>
#include <lunchbox/monitor.h>
#include <lunchbox/rng.h>

#include <iostream>

namespace
{

lunchbox::Monitor<bool> monitor( false );

static const std::string message =
    "Don't Panic! And now some more text to make the message bigger";

#ifdef COLLAGE_USE_MPI
#define NMESSAGES (unsigned) co::MPI::instance()->getSize() * 1000
#else
#define NMESSAGES 1000
#endif
}


class Server : public co::LocalNode
{
public:
    Server() : _messagesLeft( NMESSAGES ){}

    virtual bool listen()
        {
            if( !co::LocalNode::listen( ))
                return false;

            registerCommand( co::CMD_NODE_CUSTOM,
                             co::CommandFunc<Server>( this, &Server::command ),
                             getCommandThreadQueue( ));
            return true;
        }

protected:
    bool command( co::ICommand& cmd )
        {
            TEST( cmd.getCommand() == co::CMD_NODE_CUSTOM );
            TEST( _messagesLeft > 0 );

            const std::string& data = cmd.get< std::string >();
            TESTINFO( message == data, data );

            --_messagesLeft;
            if( !_messagesLeft )
                monitor.set( true );

            return true;
        }

private:
    unsigned _messagesLeft;
};

void runMPITest()
{
    lunchbox::RNG rng;
    const uint16_t port = 1024;

    if( co::MPI::instance()->getRank() == 0 )
    {
        lunchbox::RefPtr< Server > server;
        server = new Server( );
        co::ConnectionDescriptionPtr connDesc = new co::ConnectionDescription;
        connDesc->type = co::CONNECTIONTYPE_MPI;
        connDesc->port = port;
        connDesc->rank = 0;
        connDesc->setHostname( "localhost" );
        server->addConnectionDescription( connDesc );

        TEST( server->listen( ));

        MPI_Barrier(MPI_COMM_WORLD);

        TEST( server->close( ));
        TESTINFO( server->getRefCount() == 1, server->getRefCount( ));
    }
    else
    {
        co::NodePtr serverProxy = new co::Node;
        co::ConnectionDescriptionPtr connDesc = new co::ConnectionDescription;
        connDesc->type = co::CONNECTIONTYPE_MPI;
        connDesc->port = port;
        connDesc->rank = 0;
        connDesc->setHostname( "localhost" );

        serverProxy->addConnectionDescription( connDesc );

        co::LocalNodePtr client = new co::LocalNode;
        connDesc = new co::ConnectionDescription;
        connDesc->type = co::CONNECTIONTYPE_MPI;
        connDesc->rank = co::MPI::instance()->getRank();

        client->addConnectionDescription( connDesc );
        TEST( client->listen( ));
        TEST( client->connect( serverProxy ));


        lunchbox::Clock clock;
        for( unsigned i = 0; i < NMESSAGES; ++i )
            serverProxy->send( co::CMD_NODE_CUSTOM ) << message;
        const float time = clock.getTimef();

        const size_t size = NMESSAGES * ( co::OCommand::getSize() +
                                          message.length() - 7 );
        std::cout << "Send " << size << " bytes using " << NMESSAGES
                  << " commands in " << time << "ms" << " ("
                  << size / 1024. * 1000.f / time << " KB/s)" << std::endl;

        MPI_Barrier(MPI_COMM_WORLD);

        TEST( client->disconnect( serverProxy ));
        TEST( client->close( ));

        TESTINFO( serverProxy->getRefCount() == 1, serverProxy->getRefCount( ));
        TESTINFO( client->getRefCount() == 1, client->getRefCount( ));
    }
}

int main( int argc, char **argv )
{
    co::init( argc, argv );

    #ifdef COLLAGE_USE_MPI
    /* Check if started with mpirun and size of MPI_COMM_WORLD
     * is equal to 2.
     */
    if( co::MPI::instance()->supportsThreads() &&
        co::MPI::instance()->getSize() > 1 )
    {
        if( co::MPI::instance()->getSize() == 2 )
            runMPITest();
        co::exit();
        return EXIT_SUCCESS;
    }
    #endif

    lunchbox::RNG rng;
    const uint16_t port = (rng.get<uint16_t>() % 60000) + 1024;

    lunchbox::RefPtr< Server > server = new Server;
    co::ConnectionDescriptionPtr connDesc = new co::ConnectionDescription;

    connDesc->type = co::CONNECTIONTYPE_TCPIP;
    connDesc->port = port;
    connDesc->setHostname( "localhost" );
    server->addConnectionDescription( connDesc );

    TEST( server->listen( ));

    co::NodePtr serverProxy = new co::Node;
    serverProxy->addConnectionDescription( connDesc );

    connDesc = new co::ConnectionDescription;
    connDesc->type = co::CONNECTIONTYPE_TCPIP;
    connDesc->setHostname( "localhost" );

    co::LocalNodePtr client = new co::LocalNode;
    client->addConnectionDescription( connDesc );
    TEST( client->listen( ));
    TEST( client->connect( serverProxy ));

    lunchbox::Clock clock;
    for( unsigned i = 0; i < NMESSAGES; ++i )
        serverProxy->send( co::CMD_NODE_CUSTOM ) << message;
    const float time = clock.getTimef();

    const size_t size = NMESSAGES * ( co::OCommand::getSize() +
                                      message.length() - 7 );
    std::cout << "Send " << size << " bytes using " << NMESSAGES
              << " commands in " << time << "ms" << " ("
              << size / 1024. * 1000.f / time << " KB/s)" << std::endl;

    monitor.waitEQ( true );

    TEST( client->disconnect( serverProxy ));
    TEST( client->close( ));
    TEST( server->close( ));

    TESTINFO( serverProxy->getRefCount() == 1, serverProxy->getRefCount( ));
    TESTINFO( client->getRefCount() == 1, client->getRefCount( ));
    TESTINFO( server->getRefCount() == 1, server->getRefCount( ));

    serverProxy = 0;
    client      = 0;
    server      = 0;

    co::exit();
    return EXIT_SUCCESS;
}
