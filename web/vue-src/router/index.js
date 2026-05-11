import { createRouter, createWebHistory } from 'vue-router'
import Home from '../views/Home.vue'
import Logs from '../views/Logs.vue'
import Database from '../views/Database.vue'

const routes = [
  {
    path: '/',
    name: 'Home',
    component: Home
  },
  {
    path: '/logs',
    name: 'Logs',
    component: Logs
  },
  {
    path: '/database',
    name: 'Database',
    component: Database
  }
]

const router = createRouter({
  history: createWebHistory(),
  routes
})

export default router
