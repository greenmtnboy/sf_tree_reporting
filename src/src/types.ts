export interface RawTree {
  tree_id: number
  common_name: string
  q_site_info: string
  plant_date: string
  q_species: string
  latitude: number
  longitude: number
  diameter_at_breast_height: number | null
}

export type TreeCategory = 'palm' | 'broadleaf' | 'spreading' | 'coniferous' | 'columnar' | 'ornamental' | 'default'
