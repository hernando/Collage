
/* Copyright (c) 2010-2014, Stefan Eilemann <eile@equalizergraphics.com>
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

namespace
{
static co::ConnectionType types[] =
{
    co::CONNECTIONTYPE_TCPIP,
    co::CONNECTIONTYPE_MPI,
    co::CONNECTIONTYPE_NONE // must be last
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
            
    for( size_t i = 0; types[i] != co::CONNECTIONTYPE_NONE; ++i )
    {
        co::ConnectionDescriptionPtr desc = new co::ConnectionDescription;
        desc->type = types[i];
        desc->rank = 0; 
        desc->port = 1252; 
        desc->setHostname( "127.0.0.1" );

        lunchbox::Clock clock;

        if( co::Global::getMPIRank() == 0 )
        {
            co::ConnectionPtr listener = co::Connection::create( desc );
            if( !listener )
            {
                std::cout << desc->type << ": not supported" << std::endl;
                continue;
            }


            const bool listening = listener->listen();
            if( !listening )
                continue;

            TESTINFO( listening, desc );
            listener->acceptNB();
            co::ConnectionPtr reader = listener->acceptSync();
        
            TEST( reader );

            co::Buffer buffer;
            co::BufferPtr syncBuffer;

            clock.reset();
            for( size_t j = 0; j < NPACKETS; ++j )
            {
                reader->recvNB( &buffer, PACKETSIZE );
                TEST( reader->recvSync( syncBuffer ));
                TEST( syncBuffer == &buffer );
                TEST( buffer.getSize() == PACKETSIZE );
                buffer.setSize( 0 );
            }

            reader->recvNB( &buffer, PACKETSIZE );
            TEST( !reader->recvSync( syncBuffer ));
            TEST( reader->isClosed( ));

            if( listener.isValid( ))
                TEST( listener->getRefCount() == 1 );
            if( reader.isValid( ))
                TEST( reader->getRefCount() == 1 );
        }
        else
        {
            co::ConnectionPtr writer = co::Connection::create( desc );
            TEST( writer->connect( ));

            TEST( writer );
            uint8_t out[ PACKETSIZE ];

            clock.reset();
            for( size_t j = 0; j < NPACKETS; ++j )
                TEST( writer->send( out, PACKETSIZE ));

            writer->close();
        
            TEST( writer->getRefCount() == 1 );
        }

        const float time = clock.getTimef();

        std::cout << desc->type << " rank " << co::Global::getMPIRank() << ": "
                  << NPACKETS * PACKETSIZE / 1024.f / 1024.f * 1000.f / time
                  << " MB/s" << std::endl;
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
