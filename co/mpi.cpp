
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

MPI * MPI::_instance = 0;

MPI::MPI()
    : _rank( -1 )
    , _size( -1 )
    , _supportedThreads( false )
    , _init( false )
{
}

MPI::MPI( int argc, char ** argv )
    : _rank( -1 )
    , _size( -1 )
    , _supportedThreads( false )
    , _init( false )
{
	int threadSupportProvided = -1;
	if( MPI_SUCCESS != MPI_Init_thread( (int*) &argc, &argv,
                                            MPI_THREAD_MULTIPLE,
                                            &threadSupportProvided ) )
    {
        LBERROR << "Error at initialization MPI library" << std::endl;
        return;
    }

    _init = true;

	switch( threadSupportProvided )
    {
    case MPI_THREAD_SINGLE:
		LBINFO << "MPI_THREAD_SINGLE thread support" << std::endl;
        break;
	case MPI_THREAD_FUNNELED:
		LBINFO << "MPI_THREAD_FUNNELED thread support" << std::endl;
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
	}

    if( MPI_SUCCESS != MPI_Comm_rank( MPI_COMM_WORLD, &_rank ) )
    {
        LBWARN << "Error determining the rank of the calling\
                    process in the communicator." << std::endl;
    }
    if( MPI_SUCCESS != MPI_Comm_size( MPI_COMM_WORLD, &_size ) )
    {
        LBWARN << "Error determining the size of the group\
                    associated with a communicator." << std::endl;
    }
}

MPI::~MPI()
{
    if( _init )
        if( MPI_SUCCESS != MPI_Finalize() )
        {
            LBERROR << "Error at finalizing MPI library" << std::endl;
        }
}

bool MPI::supportsThreads() const
{
    LBASSERT( _init );
    return _supportedThreads;
}

int MPI::getRank() const
{
    LBASSERT( _supportedThreads );
    return _rank;
}

int MPI::getSize() const
{
    LBASSERT( _supportedThreads );
    return _size;
}

const MPI * MPI::instance(int argc, char ** argv)
{
    if( !_instance )
    {
        static MPI instance = MPI( argc, argv );
        _instance = &instance; 
    }

    return _instance;
}

}
