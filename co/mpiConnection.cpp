
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

#include <lunchbox/scopedMutex.h>

#include <boost/thread/thread.hpp>

#include <set>
#include <queue>

#define MAX_SIZE_MSG 4096
#define TIMEOUT	1
#define	MAX_TIMEOUTS 10

namespace
{
	static class TagManager
	{
		public:
			TagManager() : _nextTag(0)
			{
			}

			bool registerTag(int16_t tag)
			{
				lunchbox::ScopedMutex<> mutex( _lock );

				if (_tags.find(tag) != _tags.end())
					return false;

				_tags.insert(tag);

				return true;
			}

			void deregisterTag(int16_t tag)
			{
				lunchbox::ScopedMutex<> mutex( _lock );

				_tags.erase(tag);
			}

			int32_t getTag()
			{
				lunchbox::ScopedMutex<> mutex( _lock );

				do
				{
					_nextTag++;
					LBASSERT(_nextTag > 0)
				}
				while(_tags.find(_nextTag) != _tags.end());

				_tags.insert(_nextTag);

				return _nextTag; 
			}

		private:
			std::set<int16_t>	_tags;
			int16_t				_nextTag;
			lunchbox::Lock		_lock;
	} tagManager;
}

namespace co
{

namespace detail
{
	class AsyncConnection;

	class MPIConnection
	{
		public:
			MPIConnection() : 
							  _rank(-1)
							, _peerRank(-1)
							, _tag(-1)
							, _asyncConnection(0)
			{
				// Ask rank of the process
				if (MPI_SUCCESS != MPI_Comm_rank(MPI_COMM_WORLD, &_rank))
				{
					LBERROR << "Could not determine the rank of the calling process in the communicator: MPI_COMM_WORLD" << std::endl;
				}

				LBASSERT( _rank >= 0 );
			}

			int32_t		_rank;
			int32_t		_peerRank;
			int16_t		_tag;

			// Tags 
			std::set<int16_t>	_tags;

			std::map<void *, std::queue<MPI_Request*>>	_requests;

			AsyncConnection *				_asyncConnection;
	};

	class AsyncConnection : lunchbox::Thread
	{
		public:

			AsyncConnection(MPIConnection * detail, MPI_Request * request, int16_t tag) : 
																	  _detail(detail)
																	, _tag(tag)
																	, _status(false)
																	, _request(request)
			{
				start();
			}

			bool wait()
			{
				join();
				return _status;
			}

			MPIConnection * getImpl()
			{
				return _detail;
			}

			void run()
			{
				if (MPI_SUCCESS != MPI_Wait(_request, NULL))
				{
					LBERROR << "Error waiting for peer rank in a MPI connection."<< std::endl;
					_status = false;
					return;
				}
				delete _request;

				LBASSERT( _detail->_peerRank >= 0 );

				int cTag = tagManager.getTag();

				// Send Tag
				if (MPI_SUCCESS != MPI_Send(&cTag, 1, MPI_INT, _detail->_peerRank, _tag, MPI_COMM_WORLD))
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
					_notifier(-1),
					_impl(new detail::MPIConnection)
{
	ConnectionDescriptionPtr description = _getDescription();
	description->type = CONNECTIONTYPE_MPI;
    description->bandwidth = 1024000; // For example :S
}

MPIConnection::MPIConnection(detail::MPIConnection * impl) : 
					_notifier(-1),
					_impl(impl)
{
	ConnectionDescriptionPtr description = _getDescription();
	description->type = CONNECTIONTYPE_MPI;
    description->bandwidth = 1024000; // For example :S
}

MPIConnection::~MPIConnection()
{
	close();

	if (_impl->_asyncConnection!= 0)
		delete _impl->_asyncConnection;
	
	delete _impl;
}

bool MPIConnection::connect()
{
    LBASSERT( getDescription()->type == CONNECTIONTYPE_MPI );

    if( !isClosed() )
        return false;

    _setState( STATE_CONNECTING );

	ConnectionDescriptionPtr description = _getDescription();
	_impl->_peerRank = description->rank;
	int cTag = description->port;

	if (MPI_SUCCESS != MPI_Send(&_impl->_rank, 1, MPI_INT, _impl->_peerRank, cTag, MPI_COMM_WORLD))
	{
		LBERROR << "Could not connect to "<< _impl->_peerRank << " process."<< std::endl;
		return false;
	}

	int tag = -1;

	if (MPI_SUCCESS != MPI_Recv(&tag, 1, MPI_INT, _impl->_peerRank, cTag, MPI_COMM_WORLD, NULL))
	{
		LBERROR << "Could not receive name port from "<< _impl->_peerRank << " process."<< std::endl;
		return false;
	}

	// Check tag is correct
	LBASSERT( tag > 0 );

	// set a default tag
	_impl->_tag = tag;

    _setState( STATE_CONNECTED );

    LBINFO << "Connected with rank " <<_impl->_peerRank<<" on tag "<<_impl->_tag<< std::endl;

	return true;
}

bool MPIConnection::listen()
{
    if( !isClosed( ))
        return false;

	// Set tag for listening
	int16_t tag = getDescription()->port;

	// Register tag
	if ( !tagManager.registerTag(tag) )
	{
		LBERROR<<"Tag "<<tag<<" is already register"<<std::endl;
		return false;
	}
	_impl->_tag = tag;

	LBINFO<<"MPI Connection, rank "<<_impl->_rank<<" listening on tag "<<_impl->_tag<<std::endl;

    _setState( STATE_LISTENING );

	return true;
}

void MPIConnection::close()
{
    if( isClosed( ))
        return;

	// Deregister tags
	std::set<int16_t>::iterator it;
	for (it=_impl->_tags.begin(); it!=_impl->_tags.end(); ++it)
		tagManager.deregisterTag(*it);

    _setState( STATE_CLOSED );
}

void MPIConnection::acceptNB()
{
	// Ensure tag is register
	LBASSERT( _impl->_tag != -1 );

	// Avoid multiple accepting process at the same time
	// To start a new accept proccess first call acceptSync to finish the last one.
	LBASSERT( _impl->_asyncConnection == 0 )

	MPI_Request * request = new MPI_Request;

	detail::MPIConnection * newImpl = new detail::MPIConnection();

	// Recieve the peer rank
	if (MPI_SUCCESS != MPI_Irecv(&newImpl->_peerRank, 1, MPI_INT, MPI_ANY_SOURCE, _impl->_tag, MPI_COMM_WORLD, request))
	{
		LBERROR << "Could not start accepting a MPI connection."<< std::endl;
		close();
		return;
	}

	_impl->_asyncConnection = new detail::AsyncConnection(newImpl, request, _impl->_tag);
}

ConnectionPtr MPIConnection::acceptSync()
{
    if( !isListening( ))
        return 0;
	
	LBASSERT(_impl->_asyncConnection != 0)

	if (!_impl->_asyncConnection->wait())
	{
		LBERROR << "Error accepting a MPI connection."<< std::endl;
		close();
		return 0;
	}
	
	detail::MPIConnection * newImpl = _impl->_asyncConnection->getImpl();
	
	delete _impl->_asyncConnection;
	_impl->_asyncConnection = 0;

    MPIConnection* newConnection = new MPIConnection(newImpl);
    newConnection->_setState( STATE_CONNECTED );

    LBINFO << "Accepted to rank " << newImpl->_peerRank << " on tag "<<newImpl->_tag <<std::endl;

	return newConnection;
}

int64_t MPIConnection::_readSync(MPI_Request * request)
{
	unsigned int timeouts = MAX_TIMEOUTS;
	int flag = 0;
	MPI_Status status;

	while(timeouts)
	{
		if (MPI_SUCCESS != MPI_Test(request, &flag, &status))
		{
			LBWARN << "Read error" << lunchbox::sysError << std::endl;
			close();
			return -1;
		}

		if (flag)
			break;

		timeouts--;
		boost::this_thread::sleep(boost::posix_time::milliseconds(timeouts * TIMEOUT));
	}

	#if 0
	if (MPI_SUCCESS != MPI_Wait(request, &status))
	{
		LBWARN << "Read error" << lunchbox::sysError << std::endl;
		close();
		return -1;
	}
	#endif

	if (!flag)
	{
		MPI_Cancel(request);
		return 0;
	}

	LBASSERT( status.MPI_TAG == _impl->_tag );

	int read = -1;
	if (MPI_SUCCESS != MPI_Get_count(&status, MPI_BYTE, &read))
	{
		LBWARN << "Read error" << lunchbox::sysError << std::endl;
		close();
		return -1;
	}

	return read;
}

void MPIConnection::readNB( void* buffer, const uint64_t bytes )
{
    if( isClosed() )
        return;

	if ( bytes <= MAX_SIZE_MSG)
	{
		_impl->_requests.insert(std::pair<void*,std::queue<MPI_Request*>>(buffer, std::queue<MPI_Request*>()));

		std::queue<MPI_Request*> requestQ = _impl->_requests.find(buffer)->second;
		MPI_Request * request = new MPI_Request;
		requestQ.push(request);

		std::cout<<requestQ.size()<<std::endl;

		if (MPI_SUCCESS != MPI_Irecv(buffer, bytes, MPI_BYTE, _impl->_peerRank, _impl->_tag, MPI_COMM_WORLD, request))
		{
			LBWARN << "Read error" << lunchbox::sysError << std::endl;
			close();
		}
	}
	else
	{
	}
}

int64_t MPIConnection::readSync( void* buffer, const uint64_t bytes, const bool)
{
    if( !isConnected())
        return -1;

	if ( bytes <= MAX_SIZE_MSG)
	{
		std::map<void*,std::queue<MPI_Request*>>::iterator it = _impl->_requests.find(buffer);

		LBASSERT( it != _impl->_requests.end() )

		std::queue<MPI_Request*> requestQ = it->second;

		std::cout<<requestQ.size()<<std::endl;

		LBASSERT( requestQ.size() == 1 );

		MPI_Request * request = requestQ.front();

		const int64_t read =  _readSync(request);

		delete request;

		_impl->_requests.erase(it);

		return read;
	}
	else
	{
		return 0;
	}
}

bool MPIConnection::_write(const void* buffer, const uint64_t size)
{
	if (MPI_SUCCESS != MPI_Ssend((void*)buffer, size, MPI_BYTE, _impl->_peerRank, _impl->_tag, MPI_COMM_WORLD)) 
	//if (MPI_SUCCESS != MPI_Send((void*)((unsigned char*)buffer + offset), dim, MPI_BYTE, _impl->_peerRank, _impl->_tag, MPI_COMM_WORLD)) 
	{
		LBWARN << "Write error" << lunchbox::sysError << std::endl;
		close();
		return false;
	}

	return true;
}

int64_t MPIConnection::write( const void* buffer, const uint64_t bytes )
{
    if( !isConnected())
        return -1;

	if ( bytes <= MAX_SIZE_MSG)
	{
		if ( !_write(buffer, bytes) )
		{
			LBWARN << "Write error" << lunchbox::sysError << std::endl;
			close();
			return -1;
		}
	}
	else
	{
		#if 0
		uint64_t offset = 0;
		while(1)
		{
			uint64_t dim = offset + MAX_SIZE_MSG >= offset ? bytes - offset : MAX_SIZE_MSG;

			if ( !_write((const void*)((unsigned char*)buffer + offset), dim) )
			{
				LBWARN << "Write error" << lunchbox::sysError << std::endl;
				close();
				return -1;
			}

			offset += dim;

			if (offset >= bytes)
				break;
		}
		#endif
		return 0;
	}

	return bytes;
}

}
