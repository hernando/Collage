
/* Copyright (c) 2014, Carlos Duelo <cduelo@cesvima.upm.es>
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

#include <lunchbox/log.h>
#include <lunchbox/debug.h>

#include "mpi.h"

namespace co
{

namespace
{

static MPI _mpi;

MPI * _getInstance()
{
    return &_mpi;
}

}

MPI::MPI()
    : _rank( -1 )
    , _size( -1 )
    , _supportedThreads( false )
    , _init( false )
{
}

MPI::~MPI()
{
    if( MPI_SUCCESS != MPI_Finalize() )
    {
        LBERROR << "Error at finalizing MPI library" << std::endl;
    }
}

bool MPI::init(int argc, char ** argv)
{
    LBASSERT( !_init );

	int threadSupportProvided = -1;
	if( MPI_SUCCESS != MPI_Init_thread( (int*) &argc, &argv,
                                            MPI_THREAD_MULTIPLE,
                                            &threadSupportProvided ) )
    {
        _init = true;
        _supportedThreads = false;
        LBERROR << "Error at initialization MPI library" << std::endl;
        return false;
    }

	switch( threadSupportProvided )
    {
    case MPI_THREAD_SINGLE:
		LBINFO << "MPI_THREAD_SINGLE thread support" << std::endl;
        _supportedThreads = false;
        break;
	case MPI_THREAD_FUNNELED:
		LBINFO << "MPI_THREAD_FUNNELED thread support" << std::endl;
        _supportedThreads = false;
        break;
    case MPI_THREAD_SERIALIZED:
		LBINFO << "MPI_THREAD_SERIALIZED thread support" << std::endl;
        _supportedThreads = true;
        break;
    case MPI_THREAD_MULTIPLE:
		LBINFO << "MPI_THREAD_MULTIPLE thread support" << std::endl;
        _supportedThreads = true;
        break;
    default:
		LBERROR << "Unknown MPI thread support" << std::endl;
        _supportedThreads = false;
	}

    if( MPI_SUCCESS != MPI_Comm_rank( MPI_COMM_WORLD, &_rank ) )
    {
        LBWARN << "Error determining the rank of the calling\
                    process in the communicator." << std::endl;
        _supportedThreads = false;
    }
    if( MPI_SUCCESS != MPI_Comm_size( MPI_COMM_WORLD, &_size ) )
    {
        LBWARN << "Error determining the size of the group\
                    associated with a communicator." << std::endl;
        _supportedThreads = false;
        return false;
    }

    _init = true;

    return true;
}

bool MPI::supportsThreads()
{
    LBASSERT( _init );
    return _supportedThreads;
}

int MPI::getRank()
{
    LBASSERT( _supportedThreads );
    return _rank;
}

int MPI::getSize()
{
    LBASSERT( _supportedThreads );
    return _size;
}

MPI * MPI::instance()
{
    return _getInstance();
}

}
