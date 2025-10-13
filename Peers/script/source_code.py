#### Install packages (run once)
# pip install pandas numpy scikit-learn matplotlib seaborn scipy

#### Import libraries
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.impute import KNNImputer
from sklearn.metrics import silhouette_score
from scipy.spatial.distance import pdist, squareform
import warnings
warnings.filterwarnings('ignore')

##### Import data
peer_data = pd.read_csv("peer_data.csv")

###############################
##### Step 1: Prepare Data ####
###############################

### 1A: Remove or estimate missing data

# Option 1: Remove all rows with missing values
peer_data_clean = peer_data.dropna()

# Option 2: Impute data using KNN Imputation
imputer = KNNImputer(n_neighbors=5)
peer_data_imputed = pd.DataFrame(
    imputer.fit_transform(peer_data),
    columns=peer_data.columns
)

# Choose which dataset to use
peer_data_processed = peer_data_imputed  # or peer_data_clean

### 1B: Standardize data
scaler = StandardScaler()
scaled = scaler.fit_transform(peer_data_processed)

# Convert back to DataFrame for easier handling (optional)
scaled_df = pd.DataFrame(scaled, columns=peer_data_processed.columns)

####################################
#### Step 2: Calculate distance ####
####################################

def plot_distance_matrix(data, method='euclidean'):
    """
    Compute and plot distance matrix
    
    Parameters:
    data: scaled data array
    method: distance metric - 'euclidean', 'manhattan', 'correlation', etc.
    """
    # Compute distance matrix
    if method == 'euclidean':
        distance = pdist(data, metric='euclidean')
    elif method == 'manhattan':
        distance = pdist(data, metric='cityblock')
    elif method == 'correlation':
        distance = pdist(data, metric='correlation')
    else:
        distance = pdist(data, metric=method)
    
    # Convert to square matrix
    distance_matrix = squareform(distance)
    
    # Plot distance matrix
    plt.figure(figsize=(10, 8))
    sns.heatmap(distance_matrix, 
                cmap='RdYlBu_r',
                xticklabels=False,
                yticklabels=False,
                cbar_kws={'label': 'Distance'})
    plt.title('Distance Matrix')
    plt.tight_layout()
    plt.show()
    
    return distance_matrix

# Plot distance matrix
distance_matrix = plot_distance_matrix(scaled, method='euclidean')

##################################
#### Step 3: Cluster analysis ####
##################################

# Set random seed for reproducibility
np.random.seed(123)

# Perform k-means clustering for different k values
k2 = KMeans(n_clusters=2, n_init=25, random_state=123)
k3 = KMeans(n_clusters=3, n_init=25, random_state=123)
k4 = KMeans(n_clusters=4, n_init=25, random_state=123)
k5 = KMeans(n_clusters=5, n_init=25, random_state=123)

# Fit the models
k2.fit(scaled)
k3.fit(scaled)
k4.fit(scaled)
k5.fit(scaled)

# Get cluster labels
labels_k2 = k2.labels_
labels_k3 = k3.labels_
labels_k4 = k4.labels_
labels_k5 = k5.labels_

def plot_clusters(data, kmeans_model, k, uh_index=None):
    """
    Visualize clusters using PCA for dimensionality reduction
    
    Parameters:
    data: scaled data
    kmeans_model: fitted KMeans model
    k: number of clusters
    uh_index: index of UH in the dataset (optional)
    """
    from sklearn.decomposition import PCA
    
    # Reduce to 2 dimensions for visualization
    pca = PCA(n_components=2)
    data_2d = pca.fit_transform(data)
    
    # Create plot
    plt.figure(figsize=(8, 6))
    
    # Plot points
    scatter = plt.scatter(data_2d[:, 0], data_2d[:, 1], 
                         c=kmeans_model.labels_, 
                         cmap='viridis',
                         alpha=0.6,
                         s=50)
    
    # Plot cluster centers
    centers_2d = pca.transform(kmeans_model.cluster_centers_)
    plt.scatter(centers_2d[:, 0], centers_2d[:, 1],
               c='red', 
               marker='X',
               s=200,
               edgecolors='black',
               linewidths=2,
               label='Centroids')
    
    # Label UH if index provided
    if uh_index is not None:
        plt.annotate('UH', 
                    xy=(data_2d[uh_index, 0], data_2d[uh_index, 1]),
                    xytext=(5, 5),
                    textcoords='offset points',
                    fontsize=10,
                    fontweight='bold')
    
    plt.xlabel(f'Dim1 ({pca.explained_variance_ratio_[0]:.1%})')
    plt.ylabel(f'Dim2 ({pca.explained_variance_ratio_[1]:.1%})')
    plt.title(f'k = {k}')
    plt.colorbar(scatter, label='Cluster')
    plt.legend()
    plt.tight_layout()
    
    return data_2d, pca

# Plot all clusters in a grid
fig, axes = plt.subplots(2, 2, figsize=(14, 12))

models = [k2, k3, k4, k5]
uh_index = None  # Replace with actual index if known, e.g., 42

for idx, (ax, model) in enumerate(zip(axes.flatten(), models)):
    from sklearn.decomposition import PCA
    
    k = model.n_clusters
    pca = PCA(n_components=2)
    data_2d = pca.fit_transform(scaled)
    
    plt.sca(ax)
    scatter = ax.scatter(data_2d[:, 0], data_2d[:, 1],
                        c=model.labels_,
                        cmap='viridis',
                        alpha=0.6,
                        s=50)
    
    # Plot centroids
    centers_2d = pca.transform(model.cluster_centers_)
    ax.scatter(centers_2d[:, 0], centers_2d[:, 1],
              c='red',
              marker='X',
              s=200,
              edgecolors='black',
              linewidths=2)
    
    # Label UH if index provided
    if uh_index is not None:
        ax.annotate('UH',
                   xy=(data_2d[uh_index, 0], data_2d[uh_index, 1]),
                   xytext=(5, 5),
                   textcoords='offset points',
                   fontsize=9,
                   fontweight='bold')
    
    ax.set_xlabel(f'Dim1 ({pca.explained_variance_ratio_[0]:.1%})')
    ax.set_ylabel(f'Dim2 ({pca.explained_variance_ratio_[1]:.1%})')
    ax.set_title(f'k = {k}')
    
plt.tight_layout()
plt.show()

#### Choosing optimal clusters

def elbow_method(data, max_k=10):
    """
    Plot elbow curve for optimal k selection
    """
    inertias = []
    K_range = range(1, max_k + 1)
    
    for k in K_range:
        kmeans = KMeans(n_clusters=k, n_init=25, random_state=123)
        kmeans.fit(data)
        inertias.append(kmeans.inertia_)
    
    plt.figure(figsize=(10, 6))
    plt.plot(K_range, inertias, 'bo-')
    plt.xlabel('Number of Clusters (k)')
    plt.ylabel('Within-Cluster Sum of Squares (WSS)')
    plt.title('Elbow Method for Optimal k')
    plt.grid(True)
    plt.tight_layout()
    plt.show()
    
    return inertias

def silhouette_method(data, max_k=10):
    """
    Plot silhouette scores for optimal k selection
    """
    silhouette_scores = []
    K_range = range(2, max_k + 1)  # Silhouette needs at least 2 clusters
    
    for k in K_range:
        kmeans = KMeans(n_clusters=k, n_init=25, random_state=123)
        labels = kmeans.fit_predict(data)
        score = silhouette_score(data, labels)
        silhouette_scores.append(score)
    
    plt.figure(figsize=(10, 6))
    plt.plot(K_range, silhouette_scores, 'bo-')
    plt.xlabel('Number of Clusters (k)')
    plt.ylabel('Average Silhouette Score')
    plt.title('Silhouette Method for Optimal k')
    plt.grid(True)
    plt.tight_layout()
    plt.show()
    
    return silhouette_scores

# Run optimization methods
print("Computing elbow method...")
inertias = elbow_method(scaled, max_k=10)

print("Computing silhouette method...")
silhouette_scores = silhouette_method(scaled, max_k=10)

#### Extract cluster results

print("\n" + "="*50)
print("K-means Clustering Results (k=2)")
print("="*50)
print(f"Number of clusters: {k2.n_clusters}")
print(f"Inertia (WSS): {k2.inertia_:.2f}")
print(f"\nCluster sizes:")
unique, counts = np.unique(k2.labels_, return_counts=True)
for cluster, count in zip(unique, counts):
    print(f"  Cluster {cluster}: {count} institutions")

print("\n" + "="*50)
print("K-means Clustering Results (k=5)")
print("="*50)
print(f"Number of clusters: {k5.n_clusters}")
print(f"Inertia (WSS): {k5.inertia_:.2f}")
print(f"\nCluster sizes:")
unique, counts = np.unique(k5.labels_, return_counts=True)
for cluster, count in zip(unique, counts):
    print(f"  Cluster {cluster}: {count} institutions")

# Add cluster labels to original dataframe
peer_data_processed['cluster_k2'] = k2.labels_
peer_data_processed['cluster_k5'] = k5.labels_

# Save results to CSV
peer_data_processed.to_csv('peer_data_clustered.csv', index=False)
print("\nResults saved to 'peer_data_clustered.csv'")
