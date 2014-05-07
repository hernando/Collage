
/* Copyright (c) 2005-2012, Stefan Eilemann <eile@equalizergraphics.com>
 *                    2012, Daniel Nachbaur <danielnachbaur@gmail.com>
 *                    2014, Carlos Duelo    <cduelo@cesvima.upm.es>
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
#include <co/global.h>

#include <lunchbox/clock.h>
#include <lunchbox/monitor.h>
#include <lunchbox/rng.h>

#include <iostream>

#ifdef COLLAGE_USE_MPI

#include <mpi.h>

namespace
{

lunchbox::Monitor<bool> monitor( false );

static const std::string message =
    "Don't Panic! And now some more text to make the message bigger";
#define NMESSAGES 1000
}


class Server : public co::LocalNode
{
public:
    Server(int nMessages) : _messagesLeft( nMessages ){}

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

int main( int argc, char **argv )
{
    co::init( argc, argv );

    if( co::Global::getMPISize() == 1 )
    {
        std::cout << "Test for 2 o more MPI process." << std::endl;
        co::exit();
        return EXIT_SUCCESS;
    }

    lunchbox::RNG rng;
    const uint16_t port = 1024;

    if( co::Global::getMPIRank() == 0 )
    {
        lunchbox::RefPtr< Server > server;
        server = new Server( co::Global::getMPISize() * NMESSAGES );
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
        connDesc->rank = co::Global::getMPIRank();

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

    co::exit();
    return EXIT_SUCCESS;
}

#else

int main( int, char **)
{
    std::cout << "MPI is not supported" << std::endl;
    return EXIT_SUCCESS;
}

#endif
