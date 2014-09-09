// © 2011 IDesign Inc. 
//Questions? Comments? go to 
//http://www.idesign.net

using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.ServiceModel;
using System.ServiceModel.Channels;
using System.ServiceModel.Description;
using System.ServiceModel.Discovery;
using System.Threading;
using System.Xml;
using System.Xml.Linq;

namespace King.ServiceModelEx {

    public static class DiscoveryExtensions {
        private static Binding GetMetadataExchangeBinding(string scheme) {
            if (scheme == "net.tcp")
                return MetadataExchangeBindings.CreateMexTcpBinding();

            if (scheme == "net.pipe")
                return MetadataExchangeBindings.CreateMexNamedPipeBinding();

            if (scheme == "http")
                return MetadataExchangeBindings.CreateMexHttpBinding();

            if (scheme == "https")
                return MetadataExchangeBindings.CreateMexHttpsBinding();

            throw new ArgumentException("No metadata exchange binding for scheme : " + scheme);
        }

        public static IEnumerable<Uri> DiscoveryScopes(this ServiceEndpoint endpoint) {

            var discoveryBehavior = endpoint.Behaviors.Find<EndpointDiscoveryBehavior>();
            if (discoveryBehavior == null || discoveryBehavior.Scopes.Count == 0)
                return Enumerable.Empty<Uri>();

            return discoveryBehavior.Scopes.ToArray();
        }
        public static void EnableMex(this ServiceHost host) {
            host.Description.Behaviors.Add(new ServiceMetadataBehavior());

            foreach (Uri baseAddress in host.BaseAddresses) {
                host.AddServiceEndpoint(
                    address: "MEX",
                    binding: GetMetadataExchangeBinding(baseAddress.Scheme),
                    implementedContract: typeof(IMetadataExchange)
                );
            }
        }
    }

    public static class Discover {
        private static IEnumerable<EndpointAddress> Endpoints<T>(int maxResults, Uri scope) {

            var findCriteria = new FindCriteria(typeof(T));
            findCriteria.MaxResults = maxResults;
            if (scope != null)
                findCriteria.Scopes.Add(scope);

            using (var discoveryClient = new DiscoveryClient(new UdpDiscoveryEndpoint())) {
                var findResult = discoveryClient.Find(findCriteria);
                return findResult.Endpoints.Select(o => o.Address).ToArray();
            }
        }

        public static EndpointAddress Endpoint<T>(Uri scope = null) {
            var addresses = Endpoints<T>(1, scope);
            Debug.Assert(addresses.Count() == 1);
            return addresses.Single();
        }
        public static IEnumerable<EndpointAddress> Endpoints<T>(Uri scope = null) {
            return Endpoints<T>(int.MaxValue, scope);
        }

        public static Binding Binding<T>(Uri scope = null) {
            DiscoveryClient discoveryClient = new DiscoveryClient(new UdpDiscoveryEndpoint());

            FindCriteria criteria = FindCriteria.CreateMetadataExchangeEndpointCriteria();
            criteria.MaxResults = 1;
            if (scope != null) {
                criteria.Scopes.Add(scope);
            }
            FindResponse discovered = discoveryClient.Find(criteria);
            discoveryClient.Close();

            Debug.Assert(discovered.Endpoints.Count == 1);

            Uri mexAddress = discovered.Endpoints[0].Address.Uri;

            //ServiceEndpoint[] endpoints = MetadataHelper.GetEndpoints(mexAddress.AbsoluteUri, typeof(T));

            //Debug.Assert(endpoints.Length == 1);

            //return endpoints[0].Binding;
            throw new NotImplementedException();
        }
    }

    public abstract class DiscoverableServiceHost : ServiceHost {

        public DiscoverableServiceHost() {

        }
    }

    public static class DiscoveryHelper {

        private static IEnumerable<ServiceEndpoint> UserEndpoints(this ServiceHost host) {
            var userEndpoints =
                from endpoint in host.Description.Endpoints
                where !endpoint.IsSystemEndpoint
                where !(endpoint is DiscoveryEndpoint)
                where !(endpoint is AnnouncementEndpoint)
                where !(endpoint is ServiceMetadataEndpoint)
                select endpoint;

            return userEndpoints;
        }
        private static void AddScope(this ServiceHost host, Uri scope) {

            var scopingBehavior = new EndpointDiscoveryBehavior();
            scopingBehavior.Scopes.Add(scope);
            foreach (var scopedEndpoint in host.UserEndpoints())
                scopedEndpoint.Behaviors.Add(scopingBehavior);
        }

        public static void EnableDiscovery(this ServiceHost host, 
            bool autoAnnounceEndpoints = false, 
            Uri scope = null) {

            // add endpoint to allow discovery of services
            var discoveryEndpoint = new UdpDiscoveryEndpoint();
            host.AddServiceEndpoint(discoveryEndpoint);

            // configure announcement client to auto announce endpoints
            var discoveryBehavior = new ServiceDiscoveryBehavior();
            if (autoAnnounceEndpoints)
                discoveryBehavior.AnnouncementEndpoints.Add(new UdpAnnouncementEndpoint());
            host.Description.Behaviors.Add(discoveryBehavior);

            // add a scope to every endpoint exposing a user defined service
            if (scope != null)
                host.AddScope(scope);
        }

        private static int FindAvailablePort() {
            Mutex mutex = new Mutex(false, "ServiceModelEx.DiscoveryHelper.FindAvailablePort");

            try {
                mutex.WaitOne();
                var serverIpEndpoint = new IPEndPoint(IPAddress.Any, 0);
                using (var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)) {
                    socket.Bind(serverIpEndpoint);
                    var clientIpEndpoint = (IPEndPoint)socket.LocalEndPoint;
                    return clientIpEndpoint.Port;
                }

            } finally {
                mutex.ReleaseMutex();
            }
        }
        public static Uri FindAvailableTcpBaseAddress() {
            string machineName = Environment.MachineName;
            return new Uri("net.tcp://" + machineName + ":" + FindAvailablePort() + "/");
        }
        public static Uri FindAvailableIpcBaseAddress() {
            return new Uri("net.pipe://localhost/" + Guid.NewGuid() + "/");
        }

        public static DynamicEndpoint GetDynamicEndpoint(Type contract, Binding binding, Uri scope = null) {
            var contractDescription = ContractDescription.GetContract(contract);
            var dynamicEndpoint = new DynamicEndpoint(contractDescription, binding);

            var findCriteria = new FindCriteria();
            findCriteria.MaxResults = 1;
            if (scope != null)
                findCriteria.Scopes.Add(scope);
            dynamicEndpoint.FindCriteria = findCriteria;

            return dynamicEndpoint;
        }

        internal static AnnouncementEventArgs CreateAnnouncementArgs(
            Uri address,
            string contractName,
            string contractNamespace,
            Uri[] scopes) {

            Type type = typeof(AnnouncementEventArgs);
            ConstructorInfo constructor = type.GetConstructors(BindingFlags.Instance | BindingFlags.NonPublic)[0];

            ContractDescription contract = new ContractDescription(contractName, contractNamespace);

            ServiceEndpoint endpoint = new ServiceEndpoint(contract, null, new EndpointAddress(address));
            EndpointDiscoveryMetadata metadata = EndpointDiscoveryMetadata.FromServiceEndpoint(endpoint);

            return constructor.Invoke(new object[] { null, metadata }) as AnnouncementEventArgs;
        }
        internal static FindResponse CreateFindResponse() {
            Type type = typeof(FindResponse);
            ConstructorInfo constructor = type.GetConstructors(BindingFlags.Instance | BindingFlags.NonPublic)[0];

            return constructor.Invoke(null) as FindResponse;
        }
    }

    public struct AnnouncedEndpoint {

        internal static IEnumerable<AnnouncedEndpoint> SelectMany(AnnouncementEventArgs announcement) {
            var metadata = announcement.EndpointDiscoveryMetadata;
            var address = metadata.Address;
            var scopes = metadata.Scopes;
            var names = metadata.ContractTypeNames.Select(o => XName.Get(o.Name, o.Namespace));

            foreach (var name in names)
                yield return new AnnouncedEndpoint(name, metadata);
        }

        private XName m_contract;
        private EndpointDiscoveryMetadata m_metadata;

        public AnnouncedEndpoint(XName contract, EndpointDiscoveryMetadata metadata) {
            m_contract = contract;
            m_metadata = metadata;
        }

        public EndpointAddress Address {
            get { return m_metadata.Address; }
        }
        public Uri Uri {
            get { return Address.Uri; }
        }
        public IEnumerable<Uri> Scopes() {
            return m_metadata.Scopes;
        }
        public XName ContractName {
            get { return m_contract; }
        }

        public override bool Equals(object obj) {
            return Address.Equals(obj);
        }
        public override int GetHashCode() {
            return Address.GetHashCode();
        }
    }
    public sealed class AnnoucementServiceHost : ServiceHost {

        public delegate void AnnouncementDelegate(AnnouncedEndpoint address, bool online);

        private Dictionary<XName, HashSet<AnnouncedEndpoint>> m_announcedAddresses;
        public event AnnouncementDelegate m_announcementSubscribers = delegate { };

        public AnnoucementServiceHost()
            : this(default(AnnouncementDelegate)) {
        }
        public AnnoucementServiceHost(AnnouncementDelegate subscriber)
            : this(new AnnouncementService(), subscriber) {
        }
        private AnnoucementServiceHost(AnnouncementService service, AnnouncementDelegate subscriber)
            : base(service) {

            // open broadcast address for receipt of announcements
            AddServiceEndpoint(new UdpAnnouncementEndpoint());

            // enable concurrent receipt of annoucements
            Description.Behaviors.Find<ServiceBehaviorAttribute>().UseSynchronizationContext = false;

            // dispatch annoucements concurrently
            service.OnlineAnnouncementReceived += (s, o) => AnnoucementHandler(o, true);
            service.OnlineAnnouncementReceived += (s, o) => AnnoucementHandler(o, false);

            // remember all the announced addresses seen
            m_announcedAddresses = new Dictionary<XName, HashSet<AnnouncedEndpoint>>();

            // initialize subscribers
            m_announcementSubscribers = delegate { };
            if (subscriber != null)
                m_announcementSubscribers += subscriber;
        }

        private void AnnoucementHandler(AnnouncementEventArgs annoucement, bool online) {

            foreach (var announcedEndpoint in AnnouncedEndpoint.SelectMany(annoucement)) {

                // update cache
                lock (m_announcedAddresses) {
                    var contractName = announcedEndpoint.ContractName;
                    HashSet<AnnouncedEndpoint> endpoints;
                    if (!m_announcedAddresses.TryGetValue(contractName, out endpoints))
                        m_announcedAddresses[contractName] = endpoints = new HashSet<AnnouncedEndpoint>();

                    if (online)
                        endpoints.Add(announcedEndpoint);
                    else
                        endpoints.Remove(announcedEndpoint);
                }

                // raise event conncurrently
                foreach (var subscriber in m_announcementSubscribers.GetInvocationList()) {
                    ThreadPool.QueueUserWorkItem(
                        o => ((AnnouncementDelegate)subscriber)(announcedEndpoint, online)
                    );
                }
            }
        }

        public event AnnouncementDelegate OnAnnouncement {
            add { m_announcementSubscribers += value; }
            remove { m_announcementSubscribers -= value; }
        }
        public IEnumerable<AnnouncedEndpoint> AnnouncedEndpoints<T>(
            IEnumerable<Uri> scopes = null, bool complement = false) {

            // resolve T to contract
            var contract = ContractDescription.GetContract(typeof(T));
            if (contract == null)
                throw new ArgumentException(
                    "Could not find contract description for contract : " + typeof(T).FullName);

            // resolve contract to endpoints
            var contractName = XName.Get(contract.Name, contract.Namespace);
            HashSet<AnnouncedEndpoint> endpoints;
            lock (m_announcedAddresses) {
                if (!m_announcedAddresses.TryGetValue(contractName, out endpoints))
                    endpoints = new HashSet<AnnouncedEndpoint>();
                endpoints = new HashSet<AnnouncedEndpoint>(endpoints);
            }

            // filter by scope
            var result = endpoints.Where(o => !scopes.Except(o.Scopes()).Any());

            // complement
            if (complement)
                result = endpoints.Except(result);

            return result;
        }
    }
}


