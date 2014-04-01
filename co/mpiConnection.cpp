
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
#include "global.h"

#include <lunchbox/scopedMutex.h>

#include <boost/thread/thread.hpp>

#include <set>
#include <queue>

#define MAX_SIZE_MSG 4096
#define TIMEOUT	1
#define	MAX_TIMEOUTS 10

namespace
{
	/** 
		Every connection inside a process has a unique MPI tag.
		This class allow to register a MPI tag and get a new unique.
	*/
	static class TagManager
	{
		public:
			TagManager() : _nextTag( 0 )
			{
			}

			bool registerTag(int16_t tag)
			{
				lunchbox::ScopedMutex< > mutex( _lock );

				if ( _tags.find( tag ) != _tags.end( ) )
					return false;

				_tags.insert( tag );

				return true;
			}

			void deregisterTag(int16_t tag)
			{
				lunchbox::ScopedMutex< > mutex( _lock );

				_tags.erase( tag );
			}

			int16_t getTag()
			{
				lunchbox::ScopedMutex< > mutex( _lock );

				do
				{
					_nextTag++;
					LBASSERT( _nextTag > 0 )
				}
				while( _tags.find( _nextTag ) != _tags.end( ) );

				_tags.insert( _nextTag );

				return _nextTag; 
			}

		private:
			std::set< int16_t >	_tags;
			int16_t				_nextTag;
			lunchbox::Lock		_lock;
	} tagManager;
}

namespace co
{

namespace detail
{
	class AsyncConnection;

	/** Detail for co::MPIConnection class */
	class MPIConnection
	{
		public:
			MPIConnection() : 
							  _rank( -1 )
							, _peerRank( -1 )
							, _tag( -1 )
							, _asyncConnection( 0 )
			{
				// Ask rank of the process
				if ( MPI_SUCCESS != MPI_Comm_rank( MPI_COMM_WORLD, &_rank ) )
				{
					LBERROR << "Could not determine the rank of the calling process in the communicator: MPI_COMM_WORLD" << std::endl;
				}

				LBASSERT( _rank >= 0 );
			}

			int32_t		_rank;
			int32_t		_peerRank;
			int16_t		_tag;

			// Tags 
			std::set< int16_t >	_tags;

			std::map< void *, std::queue< MPI_Request* >* >	_requests;

			AsyncConnection *				_asyncConnection;
	};

	/** Due to accept a new connection when listenting is an asynchronous process, this class
		perform the accepting process in a different thread.
	*/
	class AsyncConnection : lunchbox::Thread
	{
		public:

			AsyncConnection(MPIConnection * detail, MPI_Request * request, int16_t tag) : 
																	  _detail( detail )
																	, _tag( tag )
																	, _status( false )
																	, _request( request )
			{
				start( );
			}

			bool wait()
			{
				join( );
				return _status;
			}

			MPIConnection * getImpl()
			{
				return _detail;
			}

			void run()
			{
				if ( MPI_SUCCESS != MPI_Wait( _request, NULL ) )
				{
					LBERROR << "Error waiting for peer rank in a MPI connection." << std::endl;
					_status = false;
					return;
				}
				delete _request;

				LBASSERT( _detail->_peerRank >= 0 );

				int16_t cTag = tagManager.getTag( );

				// Send Tag ( int16_t = 2 bytes )
				if ( MPI_SUCCESS != MPI_Send( &cTag, 2, MPI_BYTE, _detail->_peerRank, _tag, MPI_COMM_WORLD ) )
				{
					LBERROR << "Error sending name port to peer in a MPI connection."<< std::endl;
					_status = false;
					return;
				}

				_detail->_tag = cTag;

				_status = true;
			}

		private:
			MPIConnection * _detail;
			int				_tag;
			bool			_status;
			MPI_Request *	_request;
	};

}


MPIConnection::MPIConnection() : 
					_notifier( -1 ),
					_impl( new detail::MPIConnection )
{
	ConnectionDescriptionPtr description = _getDescription( );
	description->type = CONNECTIONTYPE_MPI;
    description->bandwidth = 1024000; // For example :S

	// Check if MPI is allowed
	LBASSERTINFO( co::Global::isMPIAllowed( ) , "Thread support is not provided by the MPI library, so, to avoid future errors MPI connection is disabled" );
}

MPIConnection::MPIConnection(detail::MPIConnection * impl) : 
					_notifier( -1 ),
					_impl( impl )
{
	ConnectionDescriptionPtr description = _getDescription();
	description->type = CONNECTIONTYPE_MPI;
    description->bandwidth = 1024000; // For example :S

	// Check if MPI is allowed
	LBASSERTINFO( co::Global::isMPIAllowed( ) , "Thread support is not provided by the MPI library, so, to avoid future errors MPI connection is disabled" );
}

MPIConnection::~MPIConnection()
{
	close( );

	if ( _impl->_asyncConnection != 0 )
		delete _impl->_asyncConnection;
	
	delete _impl;
}

bool MPIConnection::connect()
{
    LBASSERT( getDescription( )->type == CONNECTIONTYPE_MPI );

    if( !isClosed( ) )
        return false;

    _setState( STATE_CONNECTING );

	ConnectionDescriptionPtr description = _getDescription( );
	_impl->_peerRank = description->rank;
	int16_t cTag = description->port;

	// To connect first send the rank
	if ( MPI_SUCCESS != MPI_Send( &_impl->_rank, 1, MPI_INT, _impl->_peerRank, cTag, MPI_COMM_WORLD ) )
	{
		LBERROR << "Could not connect to "<< _impl->_peerRank << " process." << std::endl;
		return false;
	}

	int16_t tag = -1;

	// If the listener receive the rank, he should send the MPI tag used for the communication.
	if ( MPI_SUCCESS != MPI_Recv( &tag, 2, MPI_BYTE, _impl->_peerRank, cTag, MPI_COMM_WORLD, NULL ) )
	{
		LBERROR << "Could not receive name port from "<< _impl->_peerRank << " process." << std::endl;
		return false;
	}

	// Check tag is correct
	LBASSERT( tag > 0 );

	// set a default tag
	_impl->_tag = tag;

    _setState( STATE_CONNECTED );

    LBINFO << "Connected with rank " << _impl->_peerRank << " on tag "<< _impl->_tag << std::endl;

	return true;
}

bool MPIConnection::listen()
{
    if( !isClosed( ))
        return false;

	int16_t tag = getDescription()->port;

	// Register tag
	if ( !tagManager.registerTag( tag ) )
	{
		LBERROR << "Tag " << tag << " is already register." << std::endl;
		if ( isListening( ) )
			LBINFO << "Probably, listen has already called before." << std::endl;
		return false;
	}

	// Set tag for listening
	_impl->_tag = tag;

	LBINFO << "MPI Connection, rank " << _impl->_rank << " listening on tag " << _impl->_tag << std::endl;

    _setState( STATE_LISTENING );

	return true;
}

void MPIConnection::close()
{
    if( isClosed( ))
        return;

	// Deregister tags
	std::set< int16_t >::iterator it;
	for ( it = _impl->_tags.begin( ); it != _impl->_tags.end( ); ++it )
		tagManager.deregisterTag( *it );

	// Cancel and delete requests
	std::map< void*, std::queue<MPI_Request*>* >::iterator itR = _impl->_requests.begin( );
	while( itR != _impl->_requests.end( ) )
	{
		std::queue< MPI_Request* > * queue = itR->second;	
		while( queue->size( ) > 0 )
		{
			MPI_Request * request = queue->front( );
			queue->pop( );
			MPI_Cancel( request );
			delete request;
		}

		delete queue;
		itR++;
	}

    _setState( STATE_CLOSED );
}

void MPIConnection::acceptNB()
{
	// Ensure tag is register
	LBASSERT( _impl->_tag != -1 );

	// Avoid multiple accepting process at the same time
	// To start a new accept process first call acceptSync to finish the last one.
	LBASSERT( _impl->_asyncConnection == 0 )

	MPI_Request * request = new MPI_Request;

	detail::MPIConnection * newImpl = new detail::MPIConnection( );

	// Recieve the peer rank
	if ( MPI_SUCCESS != MPI_Irecv( &newImpl->_peerRank, 1, MPI_INT, MPI_ANY_SOURCE, _impl->_tag, MPI_COMM_WORLD, request ) )
	{
		LBERROR << "Could not start accepting a MPI connection." << std::endl;
        LBWARN << "Got " << lunchbox::sysError << ", closing connection" << std::endl;
		close( );
		return;
	}

	_impl->_asyncConnection = new detail::AsyncConnection( newImpl, request, _impl->_tag );
}

ConnectionPtr MPIConnection::acceptSync()
{
    if( !isListening( ))
        return 0;
	
	LBASSERT( _impl->_asyncConnection != 0 )

	if ( !_impl->_asyncConnection->wait( ) )
	{
		LBERROR << "Error accepting a MPI connection." << std::endl;
        LBWARN << "Got " << lunchbox::sysError << ", closing connection" << std::endl;
		close( );
		return 0;
	}
	
	detail::MPIConnection * newImpl = _impl->_asyncConnection->getImpl( );
	
	delete _impl->_asyncConnection;
	_impl->_asyncConnection = 0;

    MPIConnection* newConnection = new MPIConnection( newImpl );
    newConnection->_setState( STATE_CONNECTED );

    LBINFO << "Accepted to rank " << newImpl->_peerRank << " on tag " << newImpl->_tag << std::endl;

	return newConnection;
}

int64_t MPIConnection::_readSync(MPI_Request * request)
{
	unsigned int timeouts = MAX_TIMEOUTS;
	int flag = 0;
	MPI_Status status;

	while( timeouts )
	{
		if ( MPI_SUCCESS != MPI_Test(request, &flag, &status ) )
		{
			return -1;
		}

		if ( flag )
			break;

		timeouts--;
		boost::this_thread::sleep( boost::posix_time::milliseconds( timeouts * TIMEOUT ) );
	}

	if ( !flag || status.MPI_TAG != _impl->_tag)
	{
		return -1;
	}

	int read = -1;
	if ( MPI_SUCCESS != MPI_Get_count( &status, MPI_BYTE, &read ) )
	{
		return -1;
	}

	return read;
}

void MPIConnection::readNB( void* buffer, const uint64_t bytes )
{
    if( isClosed( ) )
        return;

	if ( bytes <= MAX_SIZE_MSG )
	{
		std::queue< MPI_Request* > * requestQ = new std::queue< MPI_Request* >( );
		_impl->_requests.insert( std::pair< void*, std::queue< MPI_Request* > *>( buffer, requestQ ) );
		MPI_Request * request = new MPI_Request;

		if ( MPI_SUCCESS != MPI_Irecv( buffer, bytes, MPI_BYTE, _impl->_peerRank, _impl->_tag, MPI_COMM_WORLD, request ) )
		{
			LBWARN << "Read error" << lunchbox::sysError << ", closing connection" << std::endl;
			close( );
		}

		requestQ->push( request );
	}
	else
	{
		std::queue< MPI_Request* > * requestQ = new std::queue< MPI_Request* >( );
		_impl->_requests.insert( std::pair< void*, std::queue< MPI_Request* > *>( buffer, requestQ ) );

		uint64_t offset = 0;
		while( 1 )
		{
			uint64_t dim = offset + MAX_SIZE_MSG >= bytes ? bytes - offset : MAX_SIZE_MSG;

			LBASSERT( dim <= MAX_SIZE_MSG );

			MPI_Request * request = new MPI_Request;

			if ( MPI_SUCCESS != MPI_Irecv( (char*)buffer + offset, dim, MPI_BYTE, _impl->_peerRank, _impl->_tag, MPI_COMM_WORLD, request ) )
			{
				LBWARN << "Read error" << lunchbox::sysError << ", closing connection" << std::endl;
				close( );
			}

			requestQ->push( request );

			offset += dim;

			if ( offset >= bytes )
				break;
		}
	}
}

int64_t MPIConnection::readSync( void* buffer, const uint64_t bytes, const bool)
{
    if( !isConnected( ) )
        return -1;

	int64_t rBytes = 0;

	std::map< void*,std::queue<MPI_Request*>* >::iterator it = _impl->_requests.find( buffer );

	LBASSERT( it != _impl->_requests.end( ) )

	std::queue<MPI_Request*> * requestQ = it->second;

	if ( bytes <= MAX_SIZE_MSG )
	{
		LBASSERT( requestQ->size( ) == 1 );

		MPI_Request * request = requestQ->front( );

		rBytes =  _readSync( request );

		if ( rBytes < 0 )
		{
			LBWARN << "Read error" << lunchbox::sysError << ", closing connection" << std::endl;
			close( );
			return -1;
		}
		else
		{
			requestQ->pop( );

			delete request;
		}
	}
	else
	{
		LBASSERT( requestQ->size( ) > 1 );

		while( requestQ->size( ) > 0 )
		{
			MPI_Request * request = requestQ->front( );

			int64_t pRead =  _readSync( request );

			if ( pRead < 0 )
			{
				LBWARN << "Read error" << lunchbox::sysError << ", closing connection" << std::endl;
				close( );
				return -1;
			}
			else
			{
				rBytes += pRead;
			
				requestQ->pop( );

				delete request;
			}
		}
	}

	delete requestQ;

	_impl->_requests.erase( it );

	return rBytes;
}

int64_t MPIConnection::_write(const void* buffer, const uint64_t size)
{
	if ( MPI_SUCCESS != MPI_Ssend( (void*)buffer, size, MPI_BYTE, _impl->_peerRank, _impl->_tag, MPI_COMM_WORLD ) ) 
	//if (MPI_SUCCESS != MPI_Send((void*)((unsigned char*)buffer + offset), dim, MPI_BYTE, _impl->_peerRank, _impl->_tag, MPI_COMM_WORLD)) 
	{
		return -1;
	}

	return size;
}

int64_t MPIConnection::write( const void* buffer, const uint64_t bytes )
{
    if( !isConnected( ) )
        return -1;

	int64_t wBytes = 0;

	if ( bytes <= MAX_SIZE_MSG )
	{
		wBytes = _write( buffer, bytes );

		if ( wBytes < 0 )
		{
			LBWARN << "Write error" << lunchbox::sysError << ", closing connection" << std::endl;
			close( );
			return -1;
		}
	}
	else
	{
		while(1)
		{
			uint64_t dim = wBytes + MAX_SIZE_MSG >= (int64_t)bytes ? bytes - wBytes : MAX_SIZE_MSG;

			const int64_t pWrite =  _write((const void*)((unsigned char*)buffer + wBytes), dim);
			if ( pWrite < 0 ) 
			{
				LBWARN << "Write error" << lunchbox::sysError << ", closing connection" << std::endl;
				close();
				return -1;
			}

			wBytes += pWrite;

			if ( wBytes >= (int64_t)bytes )
				break;
		}
	}

	return wBytes;
}

}
