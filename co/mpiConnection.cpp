
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
#include <lunchbox/condition.h>

#include <boost/thread/thread.hpp>

#include <set>
#include <queue>

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

	class Dispatcher : lunchbox::Thread
	{
		public:
			Dispatcher(int32_t source, int16_t tag) :
													_source( source )
												,	_tag( tag )
												,	_isStopped( true )
												,	_totalBytes( 0 )
			{
				start();
			}

			void run()
			{
				_control.lock();
				_isStopped = false;
				_control.signal();

				while( !_isStopped )
				{
					// Wait for new petitions
					_control.wait();

					// Check if not stopped and start MPI_Probe
					if ( _isStopped )
					{
						break;
					}

					MPI_Status status;

					if ( MPI_SUCCESS != MPI_Probe( _source, _tag, MPI_COMM_WORLD, &status ) ) 
					{
						LBERROR << "Error retrieving messages " << std::endl;
						_isStopped = true;
						break;
					}

					int bytes = -1;

					if ( MPI_SUCCESS != MPI_Get_count( &status, MPI_BYTE, &bytes ) )
					{
						LBERROR << "Error retrieving messages " << std::endl;
						_isStopped = true;
						break;;
					}

					LBASSERT( bytes >= 0 );

					unsigned char b[bytes];

					if ( MPI_SUCCESS != MPI_Recv( &b, bytes, MPI_BYTE, _source, _tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE ) )
					{
						LBERROR << "Error retrieving messages " << std::endl;
						_isStopped = true;
						break;
					}

					// Check if EOF, indicate that connection should be close.
					if ( bytes == 1 && b[0] == 0xFF )
					{
						LBINFO << "Received EOF, closing connection" << std::endl;
						_isStopped = true;
					}

					_buffer.lock();

						if ( _totalBytes == 0 )
						{
							_buffer.signal();
						}

						if ( _isStopped )
							_totalBytes = -1;
						else
							_totalBytes += bytes;

					_buffer.unlock();
				}
					
				_freeResources();
				_control.unlock();
			}

			int64_t readSync(const void * buffer, int64_t bytes)
			{
				// Send signal to get up dispatcher
				_buffer.lock();

				int64_t r = 0;

				if ( _totalBytes == 0 )
				{
					_control.lock();
					if ( _isStopped  && !_control.timedWait( co::Global::getTimeout() ) )
					{
						_buffer.unlock();
						return -1;
					}
					_control.signal();
					_control.unlock();

					if ( !_buffer.timedWait( co::Global::getTimeout() ) )
					{
						return -1;
					}
				}

				if ( _totalBytes < 0 )
				{
					_buffer.unlock();
					return -1;
				}

				if ( _totalBytes - bytes >= 0 )
				{
					_totalBytes -= bytes;
					r = bytes;
				}
				else
				{
					r = _totalBytes;
					_totalBytes = 0;
				}
				_buffer.unlock();

				return r;

				if (buffer)
					return bytes;
				return 0;
			}

			bool close()
			{
				_control.lock();
				if ( !_isStopped )
				{
					_isStopped = true;
					_control.signal();
				}
				_control.unlock();

				join();

				return true;
			}

		private:
			int32_t	_source;
			int16_t _tag;

			bool	_isStopped;

			int64_t _totalBytes;

			lunchbox::Condition _control;  // Allow wait for incommings petitions.
			lunchbox::Condition _buffer;   // Protect shared object buffer.

			void _freeResources()
			{
			}
	};
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
							, dispatcher( 0 )
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

			AsyncConnection *				asyncConnection;
			Dispatcher	*					dispatcher;
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
														, _request( 0 )
			{
				start( );
			}

			void abort()
			{
				if (_request != 0 )
				{
					if ( MPI_SUCCESS !=  MPI_Cancel( _request ) && MPI_Request_free(_request) )
					{
						LBWARN << "Could not start accepting a MPI connection, closing connection." << std::endl;
						_request = 0;
						_status = false;
						return;
					}
				}

				// End the thread, Not sure if is safe :S
				cancel();
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
				_request = new MPI_Request{};

				// Recieve the peer rank
				if ( MPI_SUCCESS != MPI_Irecv( &_detail->peerRank, 1, MPI_INT, MPI_ANY_SOURCE, _tag, MPI_COMM_WORLD, _request) )
				{
					LBWARN << "Could not start accepting a MPI connection, closing connection." << std::endl;
					_status = false;
					return;
				}

				if ( MPI_SUCCESS !=  MPI_Wait( _request, MPI_STATUS_IGNORE ) )
				{
					LBWARN << "Could not start accepting a MPI connection, closing connection." << std::endl;
					_request = 0;
					_status = false;
					return;
				}
		
				_request = 0;
			
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

			MPI_Request	*	_request;
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
	_close( false );

	if ( _impl->asyncConnection != 0 )
		delete _impl->asyncConnection;

	if ( _impl->dispatcher != 0 )
		delete _impl->dispatcher;
	
	delete _impl;
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

	// Creating the dispatcher
	_impl->dispatcher = new Dispatcher( _impl->peerRank, _impl->tagRecv );

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
	_close(true);
}

void MPIConnection::_close(const bool userClose)
{
    if( isClosed( ))
        return;

    _setState( STATE_CLOSING );
	
	if ( _impl->dispatcher != 0 )
	{
		if ( userClose )
		{
			// Send remote connetion EOF and close dispatcher
			unsigned char eof = 0xFF;
			if ( MPI_SUCCESS != MPI_Send( &eof, 1, MPI_BYTE, _impl->peerRank, _impl->tagSend, MPI_COMM_WORLD ) )
			{
				LBWARN << "Error sending eof to remote " << std::endl;
			}
		}
		
		// Close Dispacher
		_impl->dispatcher->close();
	}


	if ( _impl->asyncConnection != 0 )
	{
		_impl->asyncConnection->abort( );
	}

	// Deregister tags
	tagManager.deregisterTag( _impl->tagRecv );

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
	
	// Create dispatcher of new connection
	newImpl->dispatcher = new Dispatcher( newImpl->peerRank, newImpl->tagRecv );

    MPIConnection* newConnection = new MPIConnection( newImpl );
    newConnection->_setState( STATE_CONNECTED );

    LBINFO << "Accepted to rank " << newImpl->peerRank << " on tag " << newImpl->tagRecv << std::endl;

	return newConnection;
}

void MPIConnection::readNB( void* , const uint64_t)
{
    if( isClosed( ) )
        return;
}

int64_t MPIConnection::readSync( void* buffer, const uint64_t bytes, const bool)
{
    if( !isConnected( ) )
        return -1;

	return _impl->dispatcher->readSync( buffer, bytes );
}

int64_t MPIConnection::write( const void* buffer, const uint64_t bytes )
{
    if( !isConnected( ) )
        return -1;

	if ( MPI_SUCCESS != MPI_Send( (void*)buffer, bytes, MPI_BYTE, _impl->peerRank, _impl->tagSend, MPI_COMM_WORLD ) ) 
	{
		LBWARN << "Write error, closing connection" << std::endl;
		close( );
		return -1;
	}

	return bytes;
}

}
