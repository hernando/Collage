
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
				
				// TESTING
				LBASSERT( _tags.find( tag ) != _tags.end( ) );

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
							  rank( -1 )
							, peerRank( -1 )
							, tagSend( -1 )
							, tagRecv( -1 )
							, asyncConnection( 0 )
			{
				// Ask rank of the process
				if ( MPI_SUCCESS != MPI_Comm_rank( MPI_COMM_WORLD, &rank ) )
				{
					LBERROR << "Could not determine the rank of the calling process in the communicator: MPI_COMM_WORLD" << std::endl;
				}

				LBASSERT( rank >= 0 );
			}

			int32_t		rank;
			int32_t		peerRank;
			int16_t		tagSend;
			int16_t		tagRecv;

			// Tags 
			std::map< void *, std::queue< MPI_Request* >* >	requests;

			AsyncConnection *				asyncConnection;
	};

	/** Due to accept a new connection when listenting is an asynchronous process, this class
		perform the accepting process in a different thread.
	*/
	class AsyncConnection : lunchbox::Thread
	{
		public:

			AsyncConnection(MPIConnection * detail, int16_t tag) : 
														  _detail( detail )
														, _tag( tag )
														, _status( false )
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
				// Recieve the peer rank
				if ( MPI_SUCCESS != MPI_Recv( &_detail->peerRank, 1, MPI_INT, MPI_ANY_SOURCE, _tag, MPI_COMM_WORLD, NULL) )
				{
					LBWARN << "Could not start accepting a MPI connection, closing connection." << std::endl;
					_status = false;
					return;
				}

				LBASSERT( _detail->peerRank >= 0 );

				_detail->tagRecv = tagManager.getTag( );

				// Send Tag ( int16_t = 2 bytes )
				if ( MPI_SUCCESS != MPI_Send( &_detail->tagRecv, 2, MPI_BYTE, _detail->peerRank, _tag, MPI_COMM_WORLD ) )
				{
					LBWARN << "Error sending MPI tag to peer in a MPI connection."<< std::endl;
					_status = false;
					return;
				}

				if ( MPI_SUCCESS != MPI_Recv( &_detail->tagSend, 2, MPI_BYTE, _detail->peerRank, _tag, MPI_COMM_WORLD, NULL ) )
				{
					LBWARN << "Could not receive MPI tag from "<< _detail->peerRank << " process." << std::endl;
					_status = false;
					return;
				}

				// Check tag is correct
				LBASSERT( _detail->tagSend > 0 );

				_status = true;
			}

		private:
			MPIConnection * _detail;
			int				_tag;
			bool			_status;
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

	if ( _impl->asyncConnection != 0 )
		delete _impl->asyncConnection;
	
	delete _impl;
}

int MPIConnection::_getTimeOut()
{
    const uint32_t timeout = Global::getTimeout();
    return timeout == LB_TIMEOUT_INDEFINITE ? -1 : int( timeout );
}

bool MPIConnection::connect()
{
    LBASSERT( getDescription( )->type == CONNECTIONTYPE_MPI );

    if( !isClosed( ) )
        return false;

    _setState( STATE_CONNECTING );

	ConnectionDescriptionPtr description = _getDescription( );
	_impl->peerRank = description->rank;
	int16_t cTag = description->port;

	// To connect first send the rank
	if ( MPI_SUCCESS != MPI_Ssend( &_impl->rank, 1, MPI_INT, _impl->peerRank, cTag, MPI_COMM_WORLD ) )
	{
		LBWARN << "Could not connect to "<< _impl->peerRank << " process." << std::endl;
		return false;
	}

	// If the listener receive the rank, he should send the MPI tag used for send.
	if ( MPI_SUCCESS != MPI_Recv( &_impl->tagSend, 2, MPI_BYTE, _impl->peerRank, cTag, MPI_COMM_WORLD, NULL ) )
	{
		LBWARN << "Could not receive MPI tag from "<< _impl->peerRank << " process." << std::endl;
		return false;
	}

	// Check tag is correct
	LBASSERT( _impl->tagSend > 0 );

	// Get a new tag to receive and send it.
	_impl->tagRecv = tagManager.getTag( ); 
	if ( MPI_SUCCESS != MPI_Ssend( &_impl->tagRecv, 2, MPI_BYTE, _impl->peerRank, cTag, MPI_COMM_WORLD ) )
	{
		LBWARN << "Could not connect to "<< _impl->peerRank << " process." << std::endl;
		return false;
	}

    _setState( STATE_CONNECTED );

    LBINFO << "Connected with rank " << _impl->peerRank << " on tag "<< _impl->tagRecv << std::endl;

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
		LBWARN << "Tag " << tag << " is already register." << std::endl;
		if ( isListening( ) )
			LBINFO << "Probably, listen has already called before." << std::endl;
		return false;
	}

	// Set tag for listening
	_impl->tagRecv = tag;

	LBINFO << "MPI Connection, rank " << _impl->rank << " listening on tag " << _impl->tagRecv << std::endl;

    _setState( STATE_LISTENING );

	return true;
}

void MPIConnection::close()
{
    if( isClosed( ))
        return;

	// Deregister tags
	tagManager.deregisterTag( _impl->tagRecv );

	// Cancel and delete requests
	std::map< void*, std::queue<MPI_Request*>* >::iterator itR = _impl->requests.begin( );
	while( itR != _impl->requests.end( ) )
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
    LBASSERT( isListening( ));

	// Ensure tag is register
	LBASSERT( _impl->tagRecv != -1 );

	// Avoid multiple accepting process at the same time
	// To start a new accept process first call acceptSync to finish the last one.
	LBASSERT( _impl->asyncConnection == 0 )

	detail::MPIConnection * newImpl = new detail::MPIConnection( );

	_impl->asyncConnection = new detail::AsyncConnection( newImpl, _impl->tagRecv );
}

ConnectionPtr MPIConnection::acceptSync()
{
    if( !isListening( ))
        return 0;
	
	LBASSERT( _impl->asyncConnection != 0 )

	if ( !_impl->asyncConnection->wait( ) )
	{
		LBWARN << "Error accepting a MPI connection, closing connection." << std::endl;
		close( );
		return 0;
	}
	
	detail::MPIConnection * newImpl = _impl->asyncConnection->getImpl( );
	
	delete _impl->asyncConnection;
	_impl->asyncConnection = 0;

    MPIConnection* newConnection = new MPIConnection( newImpl );
    newConnection->_setState( STATE_CONNECTED );

    LBINFO << "Accepted to rank " << newImpl->peerRank << " on tag " << newImpl->tagRecv << std::endl;

	return newConnection;
}

int64_t MPIConnection::_readSync(MPI_Request * request)
{
	unsigned int timeouts = 0;
	int flag = 0;
	MPI_Status status;

	int timeout	= _getTimeOut( );

	while( timeout > 0 )
	{
		if ( MPI_SUCCESS != MPI_Test( request, &flag, &status ) )
		{
			return -1;
		}

		if ( flag )
			break;

		timeouts++;
		timeout -= timeouts * TIMEOUT;
		boost::this_thread::sleep( boost::posix_time::milliseconds( timeouts * TIMEOUT ) );
	}

	if ( !flag || status.MPI_TAG != _impl->tagRecv || timeout  < 0 )
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
		_impl->requests.insert( std::pair< void*, std::queue< MPI_Request* > *>( buffer, requestQ ) );
		MPI_Request * request = new MPI_Request;

		if ( MPI_SUCCESS != MPI_Irecv( buffer, bytes, MPI_BYTE, _impl->peerRank, _impl->tagRecv, MPI_COMM_WORLD, request ) )
		{
			LBWARN << "Read error, closing connection" << std::endl;
			close( );
		}

		requestQ->push( request );
	}
	else
	{
		std::queue< MPI_Request* > * requestQ = new std::queue< MPI_Request* >( );
		_impl->requests.insert( std::pair< void*, std::queue< MPI_Request* > *>( buffer, requestQ ) );

		uint64_t offset = 0;
		while( 1 )
		{
			uint64_t dim = offset + MAX_SIZE_MSG >= bytes ? bytes - offset : MAX_SIZE_MSG;

			LBASSERT( dim <= MAX_SIZE_MSG );

			MPI_Request * request = new MPI_Request;

			if ( MPI_SUCCESS != MPI_Irecv( (char*)buffer + offset, dim, MPI_BYTE, _impl->peerRank, _impl->tagRecv, MPI_COMM_WORLD, request ) )
			{
				LBWARN << "Read error, closing connection" << std::endl;
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

	std::map< void*,std::queue<MPI_Request*>* >::iterator it = _impl->requests.find( buffer );

	LBASSERT( it != _impl->requests.end( ) )

	std::queue<MPI_Request*> * requestQ = it->second;

	if ( bytes <= MAX_SIZE_MSG )
	{
		LBASSERT( requestQ->size( ) == 1 );

		MPI_Request * request = requestQ->front( );

		rBytes =  _readSync( request );

		if ( rBytes < 0 )
		{
			LBWARN << "Read error, closing connection" << std::endl;
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
				LBWARN << "Read error, closing connection" << std::endl;
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

	_impl->requests.erase( it );

	return rBytes;
}

int64_t MPIConnection::_write(const void* buffer, const uint64_t size)
{
	if ( MPI_SUCCESS != MPI_Send( (void*)buffer, size, MPI_BYTE, _impl->peerRank, _impl->tagSend, MPI_COMM_WORLD ) ) 
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
			LBWARN << "Write error, closing connection" << std::endl;
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
				LBWARN << "Write error, closing connection" << std::endl;
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
