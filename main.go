package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"time"

	"sync"
	"sync/atomic"

	"gopkg.in/yaml.v2"

	"github.com/envoyproxy/go-control-plane/pkg/cache"
	xds "github.com/envoyproxy/go-control-plane/pkg/server"
	"github.com/envoyproxy/go-control-plane/pkg/util"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	"github.com/envoyproxy/go-control-plane/envoy/api/v2"
	"github.com/envoyproxy/go-control-plane/envoy/api/v2/auth"
	"github.com/envoyproxy/go-control-plane/envoy/api/v2/core"
	"github.com/envoyproxy/go-control-plane/envoy/api/v2/listener"
	"github.com/envoyproxy/go-control-plane/envoy/api/v2/route"
	hcm "github.com/envoyproxy/go-control-plane/envoy/config/filter/network/http_connection_manager/v2"
	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v2"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

var (
	debug       bool
	onlyLogging bool

	localhost = "127.0.0.1"

	port uint

	mode string

	version int32

	config cache.SnapshotCache

	currentVersion int

	serviceName string
)

const (
	XdsCluster = "xds_cluster"
	Ads        = "ads"
	Xds        = "xds"
	Rest       = "rest"
	TableName  = "ConfigSource"
)

func init() {
	flag.BoolVar(&debug, "debug", true, "Use debug logging")
	flag.BoolVar(&onlyLogging, "onlyLogging", false, "Only demo AccessLogging Service")
	flag.UintVar(&port, "port", 18000, "Management server port")
	flag.StringVar(&mode, "ads", Ads, "Management server type (ads, xds, rest)")
}

type logger struct{}

func (logger logger) Infof(format string, args ...interface{}) {
	log.Infof(format, args...)
}
func (logger logger) Errorf(format string, args ...interface{}) {
	log.Errorf(format, args...)
}
func (cb *callbacks) Report() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	log.WithFields(log.Fields{"fetches": cb.fetches, "requests": cb.requests}).Info("cb.Report()  callbacks")
}
func (cb *callbacks) OnStreamOpen(id int64, typ string) {
	log.Infof("OnStreamOpen %d open for %s", id, typ)
}
func (cb *callbacks) OnStreamClosed(id int64) {
	log.Infof("OnStreamClosed %d closed", id)
}
func (cb *callbacks) OnStreamRequest(int64, *v2.DiscoveryRequest) {
	log.Infof("OnStreamRequest")
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.requests++
	if cb.signal != nil {
		close(cb.signal)
		cb.signal = nil
	}
}
func (cb *callbacks) OnStreamResponse(int64, *v2.DiscoveryRequest, *v2.DiscoveryResponse) {
	log.Infof("OnStreamResponse...")
	cb.Report()
}
func (cb *callbacks) OnFetchRequest(req *v2.DiscoveryRequest) {
	log.Infof("OnFetchRequest...")
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.fetches++
	if cb.signal != nil {
		close(cb.signal)
		cb.signal = nil
	}
}
func (cb *callbacks) OnFetchResponse(*v2.DiscoveryRequest, *v2.DiscoveryResponse) {}

type callbacks struct {
	signal   chan struct{}
	fetches  int
	requests int
	mu       sync.Mutex
}

// Hasher returns node ID as an ID
type Hasher struct {
}

// ID function
func (h Hasher) ID(node *core.Node) string {
	if node == nil {
		return "unknown"
	}
	return node.Id
}

const grpcMaxConcurrentStreams = 1000000

// RunManagementServer starts an xDS server at the given port.
func RunManagementServer(ctx context.Context, server xds.Server, port uint) {
	var grpcOptions []grpc.ServerOption
	grpcOptions = append(grpcOptions, grpc.MaxConcurrentStreams(grpcMaxConcurrentStreams))
	grpcServer := grpc.NewServer(grpcOptions...)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.WithError(err).Fatal("failed to listen")
	}

	// register services
	discovery.RegisterAggregatedDiscoveryServiceServer(grpcServer, server)
	v2.RegisterEndpointDiscoveryServiceServer(grpcServer, server)
	v2.RegisterClusterDiscoveryServiceServer(grpcServer, server)
	v2.RegisterRouteDiscoveryServiceServer(grpcServer, server)
	v2.RegisterListenerDiscoveryServiceServer(grpcServer, server)

	log.WithFields(log.Fields{"port": port}).Info("management server listening")
	go func() {
		if err = grpcServer.Serve(lis); err != nil {
			log.Error(err)
		}
	}()
	<-ctx.Done()

	grpcServer.GracefulStop()
}

// Item : Structure to hold info about new item in DynamoDB
type Item struct {
	Service string `json:"serviceID"`
	Version int    `json:"version"`
	Config  string `json:"config"`
}

func queryDB(serviceName string, currentVersion int, svc *dynamodb.DynamoDB) []Item {

	var queryInput = &dynamodb.QueryInput{
		TableName: aws.String(TableName),
		// IndexName: aws.String("serviceID"),
		KeyConditions: map[string]*dynamodb.Condition{
			"serviceID": {
				ComparisonOperator: aws.String("EQ"),
				AttributeValueList: []*dynamodb.AttributeValue{
					{
						S: aws.String(serviceName),
					},
				},
			},
			"version": {
				ComparisonOperator: aws.String("GT"),
				AttributeValueList: []*dynamodb.AttributeValue{
					{
						N: aws.String(strconv.Itoa(currentVersion)),
					},
				},
			},
		},
	}
	var response, err = svc.Query(queryInput)
	items := []Item{}

	if err != nil {
		fmt.Println(err)
	}

	err = dynamodbattribute.UnmarshalListOfMaps(response.Items, &items)

	if err != nil {
		panic(fmt.Sprintf("Failed to unmarshal Record, %v", err))
	}

	return items
}

func main() {
	flag.Parse()
	if debug {
		log.SetLevel(log.DebugLevel)
	}
	ctx := context.Background()

	signal := make(chan struct{})
	cb := &callbacks{
		signal:   signal,
		fetches:  0,
		requests: 0,
	}
	config = cache.NewSnapshotCache(mode == Ads, Hasher{}, logger{})

	srv := xds.NewServer(config, cb)

	if onlyLogging {
		cc := make(chan struct{})
		<-cc
		os.Exit(0)
	}

	// start the xDS server
	go RunManagementServer(ctx, srv, port)
	<-signal

	cb.Report()

	type Config struct {
		Clusters []struct {
			Name                 string        `yaml:"name"`
			ConnectTimeout       time.Duration `yaml:"connect_timeout"`
			Type                 string        `yaml:"type"`
			DNSLookupFamily      string        `yaml:"dns_lookup_family"`
			LbPolicy             string        `yaml:"lb_policy"`
			HTTP2ProtocolOptions struct{}      `yaml:"http2_protocol_options"`
			Hosts                []struct {
				SocketAddress struct {
					Address   string `yaml:"address"`
					PortValue uint32 `yaml:"port_value"`
				} `yaml:"socket_address"`
			} `yaml:"hosts"`
			TLSContext struct {
				Sni string `yaml:"sni"`
			} `yaml:"tls_context"`
		}
		Listeners []struct {
			Name    string `yaml:"name"`
			Address struct {
				SocketAddress struct {
					Address   string `yaml:"address"`
					PortValue uint32 `yaml:"port_value"`
				} `yaml:"socket_address"`
			} `yaml:"address"`
			FilterChains []struct {
				Filters []struct {
					Name   string `yaml:"name"`
					Config struct {
						CodecType  string `yaml:"codec_type"`
						StatPrefix string `yaml:"stat_prefix"`
						Rds        struct {
							RouteConfigName string `yaml:"route_config_name"`
							ConfigSource    struct {
								Ads struct{} `yaml:"ads"`
							} `yaml:"config_source"`
						} `yaml:"rds"`
						HTTPFilters []struct {
							Name   string   `yaml:"name"`
							Config struct{} `yaml:"config"`
						} `yaml:"http_filters"`
					} `yaml:"config"`
				} `yaml:"filters"`
			} `yaml:"filter_chains"`
		} `yaml:"listeners"`
		Routes []struct {
			Name         string `yaml:"name"`
			VirtualHosts []struct {
				Name    string   `yaml:"name"`
				Domains []string `yaml:"domains"`
				Routes  []struct {
					Match struct {
						Prefix string `yaml:"prefix"`
					} `yaml:"match"`
					Route struct {
						HostRewrite string `yaml:"host_rewrite"`
						Cluster     string `yaml:"cluster"`
					} `yaml:"route"`
				} `yaml:"routes"`
			} `yaml:"virtual_hosts"`
		} `yaml:"routes"`
	}

	// Initialize a session in us-west-2 that the SDK will use to load
	// credentials from the shared credentials file ~/.aws/credentials.
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		Config:  aws.Config{Region: aws.String("us-west-2")},
		Profile: "demo",
	}))

	// Create DynamoDB client
	svc := dynamodb.New(sess)

	for {
		atomic.AddInt32(&version, 1)
		nodeID := config.GetStatusKeys()[1]

		// ==========================Query the DynamoDB for configs================================

		configs := queryDB("Test1", currentVersion, svc)

		if len(configs) == 0 {
			time.Sleep(5 * time.Second)
			continue
		}

		// Iterate for the configs returned by DB

		for _, conf := range configs {
			var c []cache.Resource
			var l []cache.Resource
			var r []cache.Resource
			var e []cache.Resource

			var confData Config

			err := yaml.Unmarshal([]byte(conf.Config), &confData)
			if err != nil {
				log.Printf(err.Error())
			}

			// ================================Cluster===========================================

			for i := 0; i < len(confData.Clusters); i++ {
				var h []*core.Address
				for j := 0; j < len(confData.Clusters[i].Hosts); j++ {
					h = append(h, &core.Address{
						Address: &core.Address_SocketAddress{
							SocketAddress: &core.SocketAddress{
								Address:  confData.Clusters[i].Hosts[j].SocketAddress.Address,
								Protocol: core.TCP,
								PortSpecifier: &core.SocketAddress_PortValue{
									PortValue: uint32(confData.Clusters[i].Hosts[j].SocketAddress.PortValue),
								},
							},
						},
					})
				}

				log.Infof(">>>>>>>>>>>>>>>>>>> creating cluster " + confData.Clusters[i].Name)

				c = append(c, &v2.Cluster{
					Name:            confData.Clusters[i].Name,
					ConnectTimeout:  confData.Clusters[i].ConnectTimeout,
					Type:            v2.Cluster_LOGICAL_DNS,
					DnsLookupFamily: v2.Cluster_V4_ONLY,
					LbPolicy:        v2.Cluster_ROUND_ROBIN,
					Hosts:           h,
					TlsContext: &auth.UpstreamTlsContext{
						Sni: confData.Clusters[i].TLSContext.Sni,
					},
				})
			}

			// ==============================Listener===========================================

			for k := 0; k < len(confData.Listeners); k++ {
				var filterChain []listener.FilterChain
				for a := 0; a < len(confData.Listeners[k].FilterChains); a++ {
					var filters []listener.Filter
					for b := 0; b < len(confData.Listeners[k].FilterChains[a].Filters); b++ {
						log.Infof("routeConfigName: " + confData.Listeners[k].FilterChains[a].Filters[b].Config.Rds.RouteConfigName)
						rds := &hcm.Rds{
							RouteConfigName: confData.Listeners[k].FilterChains[a].Filters[b].Config.Rds.RouteConfigName,
							ConfigSource: core.ConfigSource{
								ConfigSourceSpecifier: &core.ConfigSource_Ads{
									Ads: &core.AggregatedConfigSource{},
								},
							},
						}

						log.Infof("rds: %v", rds)

						manager := &hcm.HttpConnectionManager{
							CodecType:  hcm.AUTO,
							StatPrefix: "ingress_http",
							RouteSpecifier: &hcm.HttpConnectionManager_Rds{
								Rds: rds,
							},
							HttpFilters: []*hcm.HttpFilter{{
								Name: util.Router,
							}},
						}

						pbst, err := util.MessageToStruct(manager)
						if err != nil {
							panic(err)
						}

						filters = append(filters, listener.Filter{
							Name:   util.HTTPConnectionManager,
							Config: pbst,
						})
					}

					filterChain = append(filterChain, listener.FilterChain{
						Filters: filters,
					})
				}

				log.Infof(">>>>>>>>>>>>>>>>>>> creating listener " + confData.Listeners[k].Name)

				l = append(l, &v2.Listener{
					Name: confData.Listeners[k].Name,
					Address: core.Address{
						Address: &core.Address_SocketAddress{
							SocketAddress: &core.SocketAddress{
								Protocol: core.TCP,
								Address:  confData.Listeners[k].Address.SocketAddress.Address,
								PortSpecifier: &core.SocketAddress_PortValue{
									PortValue: confData.Listeners[k].Address.SocketAddress.PortValue,
								},
							},
						},
					},
					FilterChains: filterChain,
				})
			}

			// ====================================Routes======================================

			for p := 0; p < len(confData.Routes); p++ {
				var rdsVirtualHosts []route.VirtualHost
				for q := 0; q < len(confData.Routes[p].VirtualHosts); q++ {
					var rdsRoutes []route.Route
					for w := 0; w < len(confData.Routes[p].VirtualHosts[q].Routes); w++ {
						rdsRoutes = append(rdsRoutes, route.Route{
							Match: route.RouteMatch{
								PathSpecifier: &route.RouteMatch_Prefix{
									Prefix: confData.Routes[p].VirtualHosts[q].Routes[w].Match.Prefix,
								},
							},
							Action: &route.Route_Route{
								Route: &route.RouteAction{
									HostRewriteSpecifier: &route.RouteAction_HostRewrite{
										HostRewrite: confData.Routes[p].VirtualHosts[q].Routes[w].Route.HostRewrite,
									},
									ClusterSpecifier: &route.RouteAction_Cluster{
										Cluster: confData.Routes[p].VirtualHosts[q].Routes[w].Route.Cluster,
									},
								},
							},
						})
					}
					rdsVirtualHosts = append(rdsVirtualHosts, route.VirtualHost{
						Name:    confData.Routes[p].VirtualHosts[q].Name,
						Domains: []string{"*"},

						Routes: rdsRoutes,
					})
				}

				r = append(r, &v2.RouteConfiguration{
					Name:         confData.Routes[p].Name,
					VirtualHosts: rdsVirtualHosts,
				})
			}

			// ======================Push Config to Envoy======================================

			log.Infof(">>>>>>>>>>>>>>>>>>> creating snapshot Version " + fmt.Sprint(version))
			snap := cache.NewSnapshot(fmt.Sprint(version), e, c, r, l)

			config.SetSnapshot(nodeID, snap)
			log.Printf("Config Snapshot pushed to Envoy")
			//currentVersion = conf.Version

			time.Sleep(5 * time.Second)
		}
	}
}
