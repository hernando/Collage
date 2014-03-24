
/* Copyright (c) 2005-2014, Carlos Duelo <cduelo@cesvima.upm.es>
 *
 * This file is part of Collage <https://github.com/Eyescale/Collage>
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

#include "mpiConnection.h"
#include "connectionDescription.h"

#include <mpi.h>

namespace co
{

MPIConnection::MPIConnection() : _notifier(-1)
{
	ConnectionDescriptionPtr description = _getDescription();
	description->type = CONNECTIONTYPE_MPI;
    description->bandwidth = 1024000; // For example :S
}

MPIConnection::~MPIConnection()
{
}

bool MPIConnection::connect()
{
    LBASSERT( getDescription()->type == CONNECTIONTYPE_MPI );

    if( !isClosed() )
        return false;

    _setState( STATE_CONNECTING );

	int rank = -1;
	if (MPI_SUCCESS != MPI_Rank(MPI_COMM_WORLD, &rank))
	{
        LBERROR << "Could not determine the rank of the calling process in the communicator: MPI_COMM_WORLD" << std::endl;
	}

    LBASSERT( rank >= 0 );

    ConnectionDescriptionPtr description = getDescription();
	description->port

	if (MPI_SUCCESS != MPI_Send(&rank, 1, MPI_INT, description->port, 0, MPI_COMM_WORLD))
	{
        LBERROR << "Could not connect to "<< description->port << " process."<< std::endl;
	}

    _setState( STATE_CONNECTED );

    LBINFO << "Connected " << description->toString() << std::endl;

	return true;
}

bool MPIConnection::listen()
{
    if( !isClosed( ))
        return false;

    _setState( STATE_LISTENING );

	return true;
}

void MPIConnection::close()
{
    if( isClosed( ))
        return;

    _setState( STATE_CLOSED );
}

void MPIConnection::acceptNB()
{
	/* NOP */
}

ConnectionPtr MPIConnection::acceptSync()
{
    if( !isListening( ))
        return 0;

    ConstConnectionDescriptionPtr description = getDescription();
    MPIConnection* newConnection = new MPIConnection( description->type );
    ConnectionPtr connection( newConnection ); // to keep ref-counting correct
	return 0;
}

void readNB( void* buffer, const uint64_t bytes )
{
	buffer = 0;
	if (bytes && buffer)
		return;
}

int64_t readSync( void* buffer, const uint64_t bytes, const bool block )
{
	buffer = 0;
	if (block  && buffer)
		return 0;
	return bytes;
}

int64_t write( const void* buffer, const uint64_t bytes )
{
	buffer = 0;
	if (buffer)
		return bytes;
	return 0;
}

}
