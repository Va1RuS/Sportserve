import networkx as nx
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
import os
from textwrap import wrap

class NetworkVisualizer:
    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def build_similarity_network(self, df: pd.DataFrame):
        embeddings = np.stack(df['user_embedding'].values)
        similarity_matrix = cosine_similarity(embeddings)
        nodes = df['full_name'].tolist()
        edges = [
            {
                'source': nodes[i], 
                'target': nodes[j], 
                'weight': float(similarity_matrix[i, j])
            }
            for i in range(len(df)) for j in range(i + 1, len(df))
        ]
        avg_similarity = {
            nodes[i]: np.mean(similarity_matrix[i]) for i in range(len(nodes))
        }
        return {'nodes': nodes, 'edges': edges, 'avg_similarity': avg_similarity}

    def scale_node_sizes(self, avg_similarity):
        min_sim = min(avg_similarity.values())
        max_sim = max(avg_similarity.values())
        scale_factor = 3000 / (max_sim - min_sim if max_sim != min_sim else 1)
        return {
            node: (sim - min_sim) * scale_factor + 300 for node, sim in avg_similarity.items()
        }

    def get_node_colors(self, avg_similarity):
        min_sim = min(avg_similarity.values())
        max_sim = max(avg_similarity.values())
        cmap = plt.cm.coolwarm
        return {
            node: cmap((sim - min_sim) / (max_sim - min_sim if max_sim != min_sim else 1))
            for node, sim in avg_similarity.items()
        }

    def visualize_network(self, edges, avg_similarity, title: str, output_filename: str):
        G = nx.Graph()
        G.add_weighted_edges_from(
            [(e['source'], e['target'], e['weight']) for e in edges]
        )
        
        pos = nx.spring_layout(G, k=1.2, seed=42)
        
        node_sizes = self.scale_node_sizes(avg_similarity)
        node_colors = self.get_node_colors(avg_similarity)
        
        edge_weights = [d['weight'] for _, _, d in G.edges(data=True)]
        edge_labels = {(u, v): f'{w:.2f}' for u, v, w in G.edges(data='weight')}
        
        edge_thickness = [(w - min(edge_weights)) / (max(edge_weights) - min(edge_weights) + 1e-5) * 5 + 1 for w in edge_weights]

        plt.figure(figsize=(16, 12))
        nx.draw_networkx_edges(G, pos, width=edge_thickness, alpha=0.6, edge_color='gray')
        nx.draw_networkx_nodes(G, pos, 
                               node_size=[node_sizes[node] for node in G.nodes()],
                               node_color=[node_colors[node] for node in G.nodes()],
                               edgecolors='black', alpha=0.9)
        nx.draw_networkx_labels(G, pos, font_size=9, font_weight='bold')
        nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels, font_size=8)
        
        sm = plt.cm.ScalarMappable(cmap=plt.cm.coolwarm, norm=plt.Normalize(vmin=min(avg_similarity.values()), vmax=max(avg_similarity.values())))
        sm.set_array([])
        cbar = plt.colorbar(sm, ax=plt.gca(), orientation='vertical', pad=0.02)
        cbar.set_label("Average Similarity Score", fontsize=12)
        
        plt.title(title, fontsize=16)
        plt.axis('off')
        plt.savefig(os.path.join(self.output_dir, f"{output_filename}.png"), bbox_inches='tight', dpi=300, facecolor='white')
        plt.close()

    def analyze_similarities(self, df: pd.DataFrame):
        network_data = self.build_similarity_network(df)
        all_edges = sorted(network_data['edges'], key=lambda x: x['weight'])
        avg_similarity = network_data['avg_similarity']
        
        most_similar = all_edges[-10:][::-1]
        least_similar = all_edges[:10]
        
        self.visualize_network(most_similar, avg_similarity, "Top 10 Most Similar Users", "most_similar_network")
        self.visualize_network(least_similar, avg_similarity, "Top 10 Least Similar Users", "least_similar_network")

    def _write_analysis(self, df: pd.DataFrame, edges: list, filename: str, title: str):
        name_to_desc = dict(zip(df['full_name'], df['user_description']))
        analysis_path = os.path.join(self.output_dir, filename)
        
        with open(analysis_path, 'w') as f:
            f.write(f"=== TOP 10 {title} ===\n\n")
            for edge in edges:
                f.write(f"Similarity Score: {edge['weight']:.3f}\n")
                f.write(f"User 1: {edge['source']}\n")
                f.write(f"Description: {name_to_desc[edge['source']]}\n")
                f.write(f"User 2: {edge['target']}\n")
                f.write(f"Description: {name_to_desc[edge['target']]}\n")
                f.write("-" * 50 + "\n\n")
