import matplotlib.pyplot as plt
import seaborn as sns
from typing import Dict, List
import os
from datetime import datetime
from settings import VISUALIZATIONS_PATH


class DataVisualizer:
    def __init__(self, output_dir: str = VISUALIZATIONS_PATH):
        self.output_dir = output_dir
        self._setup_style()
        os.makedirs(output_dir, exist_ok=True)
        
    def _setup_style(self):
        plt.style.use('default')  
        sns.set_theme()
        sns.set_palette("husl")
        
    def _save_plot(self, category: str, plot_type: str):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{category}_{plot_type}_{timestamp}.png"
        plt.savefig(os.path.join(self.output_dir, filename), 
                   bbox_inches='tight', 
                   dpi=300)
        plt.close()

    def visualize_category(self, category: str, data: List[Dict[str, any]]):
        values = [item['value'] for item in data]
        counts = [item['count'] for item in data]
        percentages = [item['percentage'] for item in data]

        if category == 'subscription_status':
            self._create_pie_chart(category, values, percentages)
            self._create_donut_chart(category, values, percentages)
            
        elif category == 'subscription_plan':
            self._create_horizontal_bar(category, values, counts, percentages)
            self._create_treemap(category, values, percentages)
            
        elif category == 'gender':
            self._create_vertical_bar(category, values, counts, percentages)
            self._create_pie_chart(category, values, percentages)
            
        elif category == 'payment_method':
            self._create_horizontal_bar(category, values, counts, percentages)
            self._create_lollipop_chart(category, values, percentages)

    def _create_pie_chart(self, category: str, labels: List[str], percentages: List[float]):
        plt.figure(figsize=(12, 8))
        plt.pie(percentages, labels=labels, autopct='%1.1f%%', startangle=90)
        plt.title(f'Distribution of {category.replace("_", " ").title()}')
        self._save_plot(category, 'pie')

    def _create_donut_chart(self, category: str, labels: List[str], percentages: List[float]):
        plt.figure(figsize=(12, 8))
        plt.pie(percentages, labels=labels, autopct='%1.1f%%', startangle=90, pctdistance=0.85)
        centre_circle = plt.Circle((0,0), 0.70, fc='white')
        plt.gca().add_artist(centre_circle)
        plt.title(f'Distribution of {category.replace("_", " ").title()}')
        self._save_plot(category, 'donut')

    def _create_horizontal_bar(self, category: str, labels: List[str], counts: List[int], 
                             percentages: List[float]):
        plt.figure(figsize=(12, 8))
        bars = plt.barh(labels, counts)
        
        # Add percentage labels
        for i, bar in enumerate(bars):
            width = bar.get_width()
            plt.text(width, bar.get_y() + bar.get_height()/2,
                    f'{percentages[i]:.1f}%',
                    ha='left', va='center', fontweight='bold')
        
        plt.title(f'Distribution of {category.replace("_", " ").title()}')
        plt.xlabel('Number of Users')
        self._save_plot(category, 'horizontal_bar')

    def _create_vertical_bar(self, category: str, labels: List[str], counts: List[int], 
                           percentages: List[float]):
        plt.figure(figsize=(12, 8))
        bars = plt.bar(labels, counts)
        plt.xticks(rotation=45, ha='right')
        
        # Add percentage labels
        for i, bar in enumerate(bars):
            height = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2, height,
                    f'{percentages[i]:.1f}%',
                    ha='center', va='bottom', fontweight='bold')
        
        plt.title(f'Distribution of {category.replace("_", " ").title()}')
        plt.ylabel('Number of Users')
        self._save_plot(category, 'vertical_bar')

    def _create_lollipop_chart(self, category: str, labels: List[str], percentages: List[float]):
        plt.figure(figsize=(12, 8))
        plt.vlines(x=labels, ymin=0, ymax=percentages, color='skyblue')
        plt.plot(labels, percentages, "o")
        plt.xticks(rotation=45, ha='right')
        plt.title(f'{category.replace("_", " ").title()} Distribution')
        plt.ylabel('Percentage (%)')
        self._save_plot(category, 'lollipop')

    def _create_treemap(self, category: str, labels: List[str], percentages: List[float]):
        try:
            import squarify
            plt.figure(figsize=(12, 8))
            squarify.plot(sizes=percentages, label=labels, alpha=.8)
            plt.axis('off')
            plt.title(f'{category.replace("_", " ").title()} Distribution')
            self._save_plot(category, 'treemap')
        except ImportError:
            print("squarify package not installed. Skipping treemap visualization.") 