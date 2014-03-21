
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
}

MPIConnection::~MPIConnection()
{
}

bool MPIConnection::connect()
{
	return false;
}

bool MPIConnection::listen()
{
	return false;
}

void MPIConnection::close()
{
}

void MPIConnection::acceptNB()
{
}

ConnectionPtr MPIConnection::acceptSync()
{
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
