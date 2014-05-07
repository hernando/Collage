
/* Copyright (c) 2010-2014, Stefan Eilemann <eile@equalizergraphics.com>
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

// Tests basic connection functionality
#include <test.h>
#include <co/buffer.h>
#include <co/connection.h>
#include <co/connectionDescription.h>
#include <co/connectionSet.h>
#include <co/init.h>
#include <co/global.h>

#include <lunchbox/clock.h>
#include <lunchbox/monitor.h>
#include <iostream>

#ifdef COLLAGE_USE_MPI

#include <mpi.h>


#define PACKETSIZE (12345)
#define NPACKETS   (23456)
#define NCONNECTIONS (10)

namespace
{
uint8_t out[ PACKETSIZE ];

class Writer : public lunchbox::Thread
{
public:
    Writer()
        : runtime( 0.f )
    {}

    void startSend( co::ConnectionPtr connection )
    {
        connection_ = connection;
        TEST( start( ));
    }

    void run() override
    {
        lunchbox::Clock clock;
        for( size_t j = 0; j < NPACKETS; ++j )
            TEST( connection_->send( out, PACKETSIZE ));
        runtime = clock.getTimef();
        connection_ = 0;
    }

    float runtime;

private:
    co::ConnectionPtr connection_;
};

}

int main( int argc, char **argv )
{
    co::init( argc, argv );

    /** Check MPI Comm Size is 2 */
    if( co::Global::getMPISize() != 2 )
    {
        if( co::Global::getMPIRank() == 0 )
            std::cout << "Test for two MPI process." << std::endl;
        co::exit();
        return EXIT_SUCCESS;
    }

    co::ConnectionDescriptionPtr desc = new co::ConnectionDescription;
    desc->type = co::CONNECTIONTYPE_MPI;
    desc->rank = 0;
    desc->port = 1234;
    desc->setHostname( "127.0.0.1" );

    if( co::Global::getMPIRank() == 0 )
    {
        co::ConnectionPtr listener = co::Connection::create( desc );
        TEST( listener );
        TEST( listener->listen( ));

        co::ConnectionSet set;

        for( size_t i = 0; i < NCONNECTIONS; ++i )
        {
            listener->acceptNB();
            co::ConnectionPtr reader = listener->acceptSync();
            TEST( reader );

            set.addConnection( reader );
        }

        co::Buffer buffer;
        co::BufferPtr syncBuffer;

        for( size_t i = 0; i < NPACKETS * NCONNECTIONS ; ++i )
        {
            const co::ConnectionSet::Event result = set.select();
            TESTINFO( result == co::ConnectionSet::EVENT_DATA, result );

            co::ConnectionPtr connection = set.getConnection();
            connection->recvNB( &buffer, PACKETSIZE );
            TEST( connection->recvSync( syncBuffer ));
            TEST( syncBuffer == &buffer );
            TEST( buffer.getSize() == PACKETSIZE );
            buffer.setSize( 0 );
        }

        const co::Connections& connections = set.getConnections();
        while( !connections.empty( ))
        {
            co::ConnectionPtr connection = connections.back();
            connection->close();
            TEST( set.removeConnection( connection ));
        }
    }
    else
    {
        co::ConnectionPtr writers[ NCONNECTIONS ];
        Writer threads[ NCONNECTIONS ];

        for( size_t i = 0; i < NCONNECTIONS; ++i )
        {

            writers[i] = co::Connection::create( desc );
            TEST( writers[i]->connect( ));

            threads[i].startSend( writers[i] );
        }

        const float runtime = threads[0].runtime;
        const float delta = runtime / 10.f;
        for( size_t i = 0; i < NCONNECTIONS; ++i )
        {
            threads[i].join();
            writers[i]->close();
            TESTINFO( std::abs( threads[i].runtime - runtime ) < delta ,
                      threads[i].runtime << " != " << runtime );
        }

    }

    co::exit();
    return EXIT_SUCCESS;
}

#else

int main( int argc, char **argv )
{
    std::cout << "MPI is not supported" << std::endl;
    return EXIT_SUCCESS;
}

#endif
